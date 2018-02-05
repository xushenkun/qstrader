#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import sqlite3
import logging.config
from abc import ABCMeta, abstractmethod

import scrapy
from scrapy import Item, Field
from scrapy.exceptions import DropItem
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..' + os.path.sep + '..')
from spider.base import AbstractSpider
from util.common import load_classes

class FinBulletinItem(Item):
    seed = Field()
    bid = Field()
    title = Field()
    url = Field()
    time = Field()
    content = Field()
    def __str__(self):
        return '%s %s %s' % (self['seed'], self['bid'], self['title'])

class FinBulletinParser(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def start_requests(self):
        raise NotImplementedError("Should implement start_requests()")

    @abstractmethod
    def _parse_list(self, response):
        raise NotImplementedError("Should implement _parse_list(response)")

    @abstractmethod
    def _parse_detail(self, response):
        raise NotImplementedError("Should implement _parse_detail(response)")

class FinBulletinPipeline(object):
    def open_spider(self, spider):
        mode = 'w+' if spider.full else 'a+'
        self.file = open(spider.out_raw_file, mode=mode, encoding='utf-8')
        if spider.full:
            if os.path.exists(spider.out_db_file): os.remove(spider.out_db_file)
            self.conn = sqlite3.connect(spider.out_db_file)
            self.conn.execute("""create table finnews (seed varchar(16), nid varchar(256), title varchar(256), url varchar(256) primary key, time varchar(16))""")
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(spider.out_db_file)

    def process_item(self, item, spider):
        if item is None:
            raise DropItem("Missing item")
        elif item['title']:
            if spider.filter_by_content(item):
                raise DropItem("Filter item by content in %s" % item)
            else:
                line = "%s\t%s\t%s\t%s\t%s\t%s\n" % (item.get('seed'), item.get('nid'), item.get('title'), item.get('url'), item.get('time'), item.get('content'))
                self.file.write(line)
                self.conn.execute('insert into finnews values(?,?,?,?,?)', (item.get('seed'), item.get('nid'), item.get('title'), item.get('url'), item.get('time')))
                return item
        else:
            raise DropItem("Missing title in %s" % item)

    def close_spider(self, spider):
        self.file.close()
        self.conn.commit()
        self.conn.close()

class FinNewsSpider(AbstractSpider):

    name = "Finance News Spider"
    custom_settings = {'ITEM_PIPELINES': {'fin_news_spider.FinNewsPipeline': 100}, 'LOG_LEVEL': 'DEBUG', 'DOWNLOAD_DELAY': 0.25}

    def __init__(self, global_conf, full, conf, logger, *args, **kwargs):    
        self.out_root_path = global_conf['out_path']
        self.lock_timeout = global_conf['lock_timeout']
        self.full = full
        self.ids_seen = set()
        self.config(conf)
        super(FinNewsSpider, self).__init__(*args, **kwargs)
        FinNewsSpider.logger = logger if logger is not None else logging.getLogger('spider')
        self.an_logger = FinNewsSpider.logger

    def config(self, conf):
        self.parser_confs = conf['parsers']
        self.parser_classes = load_classes('fin_news', FinNewsParser)
        self.title_filters = conf['title_filters']
        self.content_filters = conf['content_filters']
        self.out_path = os.path.join(self.out_root_path, conf.get('out_folder'))
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_db_file = os.path.join(self.out_path, conf['out_db_file'])
        self.out_raw_file = os.path.join(self.out_path, conf['out_raw_file'])        
        if not self.full and os.path.exists(self.out_db_file):
            conn = sqlite3.connect(self.out_db_file)
            cursor = conn.cursor()
            cursor.execute("select nid from finnews")
            nids = cursor.fetchall()
            for nid in nids:
                self.ids_seen.add(nid[0])
            conn.close()

    def start_requests(self):
        self.parsers = []
        lock = filelock.FileLock(self.out_lock_file)
        with lock.acquire(timeout=self.lock_timeout):
            for parser_conf in self.parser_confs:
                enable = parser_conf.get("enable", True)
                name = parser_conf.get('name', None)
                if enable and self.parser_classes.get(name) is not None:
                    parser = self.parser_classes[name](self, parser_conf)
                    self.parsers.append(parser)
                    for req in parser.start_requests():
                        yield req

    def _exact_detail(self, css_path, response, use_auto=True):
        texts = response.css(css_path).css('::text').extract()
        texts = [t.strip() for t in texts] if texts else ''
        texts = ''.join(texts) if texts else ''
        texts = texts.replace('\n','').replace('\r','').strip()
        end = texts.rfind('责任编辑：')
        if end != -1:
            texts = texts[:end-1]
        if texts:
            item = response.meta.get('item')
            if item is not None:
                item['content'] = texts
                return item
        elif use_auto:
            return self.fulltext(response)
        else:
            return None

    def _filter_by_title(self, fni):
        if fni['url'] and fni['url'].startswith("http"):
            if fni['nid'] not in self.ids_seen:
                filtered = False
                for title_filter in self.title_filters:
                    if title_filter in fni['title']:
                        filtered = True
                if not filtered:
                    self.ids_seen.add(fni['nid'])
                    return fni
        return None

    def filter_by_content(self, fni):
        filtered = False
        for content_filter in self.content_filters:
            if content_filter in fni['content']:
                filtered = True
        return filtered

    def _jsonp(self, jsonp, end=0):
        try:
            l_index = jsonp.index('(') + 1
            r_index = jsonp.rindex(')', 0, end) if end != 0 else jsonp.rindex(')')
        except ValueError:
            return jsonp
        return jsonp[l_index:r_index]
