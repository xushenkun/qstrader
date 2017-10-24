#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import sqlite3
import datetime

import scrapy
from scrapy import Item, Field
from scrapy.exceptions import DropItem
import demjson

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from base import AbstractSpider, Spiders

NEWS_SEEDS = ["http://roll.hexun.com/roolNews_listRool.action?type=all&ids=100,101,103,125,105,124,162,194,108,122,121,119,107,116,114,115,182,120,169,170,177,180,118,190,200,155,130,117,153,106", ]
FULL_MAX_PAGE = 1#00
INCR_MAX_PAGE = 1#0
NEWS_FILE = 'finnews.txt'
DB_FILE = 'finnews.db'

class FinNewsSpider(AbstractSpider):
    name = 'Finance News Spider'
    custom_settings = {'ITEM_PIPELINES': {'fin_news_spider.FinNewsPipeline': 100},}

    def __init__(self, data_path, full, *args, **kwargs): 
        super(FinNewsSpider, self).__init__(*args, **kwargs)
        self.data_path = data_path
        self.full = full
        self.seeds = NEWS_SEEDS
        self.max_page = FULL_MAX_PAGE if full else INCR_MAX_PAGE
        self.ids_seen = set()
        db_file_path = os.path.join(self.data_path, DB_FILE)
        if not self.full and os.path.exists(db_file_path):
            conn = sqlite3.connect(db_file_path)
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
            for it in items:
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

class FinNewsPipeline(object):
    def open_spider(self, spider):
        mode = 'w+' if spider.full else 'a+'
        self.file = open(os.path.join(spider.data_path, NEWS_FILE), mode=mode, encoding='utf-8')
        db_file_path = os.path.join(spider.data_path, DB_FILE)
        if spider.full:
            if os.path.exists(db_file_path): os.remove(db_file_path)
            self.conn = sqlite3.connect(db_file_path)
            self.conn.execute("""create table finnews (nid varchar(16), title varchar(256), url varchar(256) primary key, time varchar(16))""")
            self.conn.commit()
        else:
            self.conn = sqlite3.connect(db_file_path)

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

if __name__ == '__main__':
    assert len(sys.argv) >= 2, "FinNewsSpider script should have enought arguments like: python fin_news_spider.py data_path [full]"
    data_path = sys.argv[1]
    full = False
    if len(sys.argv) >= 3:
        full = True if sys.argv[2] == 'full' else False
    spiders = Spiders([FinNewsSpider], data_path=data_path, full=full)
    spiders.start()