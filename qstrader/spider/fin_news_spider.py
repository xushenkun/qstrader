#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import sqlite3
import datetime
import logging.config

import scrapy
from scrapy import Item, Field
from scrapy.exceptions import DropItem
import demjson
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from spider.base import AbstractSpider

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
        self.seeds = conf['seeds']
        self.title_filters = conf['title_filters']
        self.out_path = os.path.join(self.out_root_path, conf.get('out_folder'))
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_db_file = os.path.join(self.out_path, conf['out_db_file'])
        self.out_news_file = os.path.join(self.out_path, conf['out_news_file'])        
        if not self.full and os.path.exists(self.out_db_file):
            conn = sqlite3.connect(self.out_db_file)
            cursor = conn.cursor()
            cursor.execute("select nid from finnews")
            nids = cursor.fetchall()
            for nid in nids:
                self.ids_seen.add(nid[0])
            conn.close()

    def start_requests(self):
        lock = filelock.FileLock(self.out_lock_file)
        with lock.acquire(timeout=self.lock_timeout):
            for seed in self.seeds:
                enable = seed.get("enable", True)
                if enable:
                    max_page = seed['full_max_page'] if self.full else seed['incr_max_page']
                    if seed['name'] == 'hexun':
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p, datetime.datetime.now().strftime('%Y-%m-%d'))
                            yield scrapy.Request(url=url, meta={'seed': seed['name']}, callback=self.parse)
                    elif seed['name'] == 'netease_finance':
                        page_num = seed['page_num']
                        for p in range(max_page, -1, -1):
                            url = seed['url'] % (p*page_num, page_num)
                            yield scrapy.Request(url=url, meta={'seed': seed['name']}, callback=self.parse)

    def parse(self, response):
        seed = response.meta.get('seed')
        if seed == 'hexun':
            rsp = demjson.decode(response.body_as_unicode())
            items = rsp.get('list', None)
            if items is not None:
                for it in sorted(items, key=lambda x: x['time']):
                    fni = FinNewsItem()
                    fni['seed'] = seed
                    fni['id'] = it.get('id')
                    fni['title'] = it.get('title')
                    fni['url'] = it.get('titleLink')
                    fni['time'] = it.get('time')
                    fni = self._filter(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self.detail)
                    else:
                        continue
        elif seed == 'netease_finance':
            rsp = self._jsonp(response.body_as_unicode())
            rsp = demjson.decode(rsp)
            items = rsp.get('BA8EE5GMwangning', None)            
            if items is not None:
                for it in sorted(items, key=lambda x: x['ptime']):                    
                    fni = FinNewsItem()
                    fni['seed'] = seed
                    fni['id'] = it.get('docid')
                    fni['title'] = it.get('title')
                    fni['url'] = it.get('url')
                    fni['time'] = it.get('ptime')
                    fni = self._filter(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self.detail)
                    else:
                        continue

    def _filter(self, fni):
        if fni['url'] and fni['url'].startswith("http"):
            if fni['id'] not in self.ids_seen:
                filtered = False
                for title_filter in self.title_filters:
                    if title_filter in fni['title']:
                        filtered = True
                if not filtered:
                    self.ids_seen.add(fni['id'])
                    return fni
        return None

    def _jsonp(self, jsonp):
        try:
            l_index = jsonp.index('(') + 1
            r_index = jsonp.rindex(')')
        except ValueError:
            return jsonp        
        return jsonp[l_index:r_index]

class FinNewsItem(Item):
    seed = Field()
    id = Field()
    title = Field()
    url = Field()
    time = Field()
    content = Field()
    def __str__(self):
        return '%s %s %s' % (self['seed'], self['id'], self['title'])

class FinNewsPipeline(object):
    def open_spider(self, spider):
        mode = 'w+' if spider.full else 'a+'
        self.file = open(spider.out_news_file, mode=mode, encoding='utf-8')
        if spider.full:
            if os.path.exists(spider.out_db_file): os.remove(spider.out_db_file)
            self.conn = sqlite3.connect(spider.out_db_file)
            self.conn.execute("""create table finnews (seed varchar(16), nid varchar(16), title varchar(256), url varchar(256) primary key, time varchar(16))""")
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(spider.out_db_file)

    def process_item(self, item, spider):
        if item['title']:
            line = "%s\t%s\t%s\t%s\t%s\t%s\n" % (item.get('seed'), item.get('id'), item.get('title'), item.get('url'), item.get('time'), item.get('content'))
            self.file.write(line)
            self.conn.execute('insert into finnews values(?,?,?,?,?)', (item.get('seed'), item.get('id'), item.get('title'), item.get('url'), item.get('time')))
            return item
        else:
            raise DropItem("Missing title in %s" % item)

    def close_spider(self, spider):
        self.file.close()
        self.conn.commit()
        self.conn.close()