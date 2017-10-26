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

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from base import AbstractSpider, Spiders

class FinNewsSpider(AbstractSpider):

    name = "Finance News Spider"
    custom_settings = {'ITEM_PIPELINES': {'fin_news_spider.FinNewsPipeline': 100}, 'LOG_LEVEL': 'DEBUG', 'DOWNLOAD_DELAY': 0.25}

    def __init__(self, global_conf, full, conf, logger, *args, **kwargs):    
        self.out_root_path = global_conf['out_path']
        self.full = full
        self.ids_seen = set()
        self.config(conf)
        super(FinNewsSpider, self).__init__(*args, **kwargs)
        self.logger = logger if logger is not None else logging.getLogger('spider')

    def config(self, conf):
        self.seeds = conf['seeds']
        self.out_path = os.path.join(self.out_root_path, conf.get('out_folder'))
        self.out_db_file = os.path.join(self.out_path, conf['out_db_file'])
        self.out_news_file = os.path.join(self.out_path, conf['out_news_file'])
        self.max_page = conf['full_max_page'] if self.full else conf['incr_max_page']
        if not self.full and os.path.exists(self.out_db_file):
            conn = sqlite3.connect(self.out_db_file)
            cursor = conn.cursor()
            cursor.execute("select nid from finnews")
            nids = cursor.fetchall()
            for nid in nids:
                self.ids_seen.add(nid[0])
            conn.close()

    def start_requests(self):
        for p in range(self.max_page, 0, -1):
            for seed in self.seeds:
                url = "%s&page=%d&date=%s" % (seed, p, datetime.datetime.now().strftime('%Y-%m-%d'))
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        rsp = demjson.decode(response.body_as_unicode())
        items = rsp.get('list', None)
        if items is not None:
            for it in sorted(items, key=lambda x: x['time']):
                item = FinNewsItem()
                item['id'] = it.get('id')
                item['title'] = it.get('title')
                item['url'] = it.get('titleLink')
                item['time'] = it.get('time')
                if item['id'] not in self.ids_seen:
                    self.ids_seen.add(item['id'])
                    yield scrapy.Request(url=item['url'], meta={'item': item}, callback=self.detail)

class FinNewsItem(Item):
    id = Field()
    title = Field()
    url = Field()
    time = Field()
    content = Field()
    def __str__(self):
        return '%s %s' % (self['id'], self['title'])

class FinNewsPipeline(object):
    def open_spider(self, spider):
        mode = 'w+' if spider.full else 'a+'
        self.file = open(spider.out_news_file, mode=mode, encoding='utf-8')
        if spider.full:
            if os.path.exists(spider.out_db_file): os.remove(spider.out_db_file)
            self.conn = sqlite3.connect(spider.out_db_file)
            self.conn.execute("""create table finnews (nid varchar(16), title varchar(256), url varchar(256) primary key, time varchar(16))""")
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(spider.out_db_file)

    def process_item(self, item, spider):
        if item['title']:
            line = "%s\t%s\t%s\t%s\t%s\n" % (item.get('id'), item.get('title'), item.get('url'), item.get('time'), item.get('content'))
            self.file.write(line)
            self.conn.execute('insert into finnews values(?,?,?,?)', (item.get('id'), item.get('title'), item.get('url'), item.get('time')))
            return item
        else:
            raise DropItem("Missing title in %s" % item)

    def close_spider(self, spider):
        self.file.close()
        self.conn.commit()
        self.conn.close()