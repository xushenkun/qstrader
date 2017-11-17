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
from scrapy.selector import Selector
from scrapy.exceptions import DropItem
from bs4 import BeautifulSoup
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
        self.parsers = conf['parsers']
        self.title_filters = conf['title_filters']
        self.content_filters = conf['content_filters']
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
            for seed in self.parsers:
                enable = seed.get("enable", True)
                if enable:
                    max_page = seed['full_max_page'] if self.full else seed['incr_max_page']
                    if seed['name'] == 'hexun':
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p, datetime.datetime.now().strftime('%Y-%m-%d'))
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'netease':
                        page_num = seed['page_num']
                        for p in range(max_page, -1, -1):
                            url = seed['url'] % (p*page_num, page_num)
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'xueqiu':
                        url = 'https://xueqiu.com/' #for cookie
                        yield scrapy.Request(url=url, meta={'seed': seed, 'max_page': max_page, 'curr_page': -1}, callback=self.parse)  
                    elif seed['name'] == 'eastmoney':
                        urls = seed['url']
                        for p in range(max_page, 0, -1):
                            for url in urls:
                                url = url % (p)
                                yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)                      
                    elif seed['name'] == 'sina':
                        page_num = seed['page_num']
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (page_num, p, int(datetime.datetime.now().timestamp()*1000))
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'qq':
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p, int(datetime.datetime.now().timestamp()*1000))
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse, headers={'Referer':'http://roll.finance.qq.com/'})
                    elif seed['name'] == 'ifeng':
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p)
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'cnfol':
                        page_num = seed['page_num']
                        now = int(datetime.datetime.now().timestamp()*1000)
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (now/1000-14400, now/1000, page_num, p, now)
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'sohu':
                        page_num = seed['page_num']
                        now = int(datetime.datetime.now().timestamp()*1000)
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p, page_num, now)
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)
                    elif seed['name'] == 'cnstock':
                        page_num = seed['page_num']
                        for p in range(max_page, 0, -1):
                            url = seed['url'] % (p)
                            yield scrapy.Request(url=url, meta={'seed': seed}, callback=self.parse)

    def parse(self, response):
        seed = response.meta.get('seed')
        if seed['name'] == 'hexun':
            rsp = demjson.decode(response.body_as_unicode())
            items = rsp.get('list', None)
            if items is not None:
                current = datetime.datetime.now()
                for it in sorted(items, key=lambda x: x['time']):
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], it.get('id'))
                    fni['title'] = it.get('title')
                    fni['url'] = it.get('titleLink')
                    year = current.year if current.month >= int(it.get('time')[:2]) else current.year-1
                    fni['time'] = "%s-%s"%(year, it.get('time'))
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._hexun_detail)
                    else:
                        continue
        elif seed['name'] == 'netease':
            rsp = self._jsonp(response.body_as_unicode())
            rsp = demjson.decode(rsp)
            items = rsp.get('BA8EE5GMwangning', None)            
            if items is not None:
                for it in sorted(items, key=lambda x: x['ptime']):                    
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], it.get('docid'))
                    fni['title'] = it.get('title')
                    fni['url'] = it.get('url')
                    fni['time'] = it.get('ptime')
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self.detail)
                    else:
                        continue
        elif seed['name'] == 'xueqiu':
            if response.meta.get('curr_page') == -1:
                url = seed['url'] % (seed['page_num'], -1)
                yield scrapy.Request(url=url, meta={'seed': seed, 'max_page': response.meta.get('max_page'), 'curr_page': 0}, callback=self.parse)
            else:        
                rsp = demjson.decode(response.body_as_unicode())        
                if response.meta.get('curr_page') < response.meta.get('max_page'):
                    if rsp.get('next_max_id', 1) > 1:
                        url = seed['url'] % (seed['page_num'], rsp.get('next_max_id', 1))
                        yield scrapy.Request(url=url, meta={'seed': seed, 'max_page': response.meta.get('max_page'), 'curr_page': response.meta.get('curr_page')+1}, callback=self.parse)                
                items = rsp.get('list', None)
                if items is not None:
                    for it in items:
                        fni = FinNewsItem()
                        fni['seed'] = seed['name']                    
                        data = demjson.decode(it.get('data'))
                        fni['nid'] = "%s%s"%(seed['name'], data.get('id'))
                        fni['title'] = data.get('title')
                        fni['url'] = "https://xueqiu.com%s" % data.get('target')
                        fni['time'] = datetime.datetime.fromtimestamp(float(data.get('created_at'))/1000).strftime('%Y-%m-%d %H:%M:%S')
                        fni = self._filter_by_title(fni)
                        if fni: 
                            yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self.detail)
                        else:
                            continue
        elif seed['name'] == 'eastmoney':
            current = datetime.datetime.now()
            for text in response.css('ul#newsListContent>li>div.text'):
                fni = FinNewsItem()
                fni['seed'] = seed['name']
                fni['title'] = text.css('p.title>a::text').extract_first().strip()
                fni['url'] = text.css('p.title>a::attr(href)').extract_first().strip()
                fni['time'] = text.css('p.time::text').extract_first().strip()
                fni['time'] = fni['time'].replace('月','-').replace('日','')
                year = current.year if current.month >= int(fni['time'][:2]) else current.year-1
                fni['time'] = "%s-%s"%(year, fni['time'])
                fni['nid'] = "%s%s"%(seed['name'], fni['url'])
                fni = self._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._eastmoney_detail)
                else:
                    continue
        elif seed['name'] == 'sina':
            rsp = self._jsonp(response.body_as_unicode(), end=-7)
            rsp = demjson.decode(rsp)
            items = rsp.get('result', None)
            items = items.get('data', None) if items is not None else None        
            if items is not None:
                for it in sorted(items, key=lambda x: x['ctime']):                    
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], it.get('docid'))
                    fni['title'] = it.get('title')
                    fni['url'] = it.get('url')
                    fni['time'] = datetime.datetime.fromtimestamp(float(it.get('ctime'))).strftime('%Y-%m-%d %H:%M:%S')
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._sina_detail)
                    else:
                        continue
        elif seed['name'] == 'qq':
            current = datetime.datetime.now()
            rsp = demjson.decode(response.body_as_unicode())
            data = rsp.get('data', None)
            info = data.get('article_info', None) if data is not None else None
            lis = Selector(text=info).css('ul>li') if info is not None else None
            if lis is not None:
                for it in lis:    
                    ctime = it.css('span.t-time::text').extract_first()
                    title = it.css('a::text').extract_first()
                    href = it.css('a::attr(href)').extract_first()
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], href)
                    fni['title'] = title
                    fni['url'] = href
                    year = current.year if current.month >= int(ctime[:2]) else current.year-1
                    fni['time'] = "%s-%s"%(year, ctime)
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._qq_detail)
                    else:
                        continue
        elif seed['name'] == 'ifeng':
            for text in response.css('div.box_mid div.box_list_word'):
                fni = FinNewsItem()
                fni['seed'] = seed['name']
                fni['title'] = text.css('h2>a::text').extract_first().strip()
                fni['url'] = text.css('h2>a::attr(href)').extract_first().strip()
                fni['time'] = text.css('div.keywords>a::text').extract_first().strip()
                fni['nid'] = "%s%s"%(seed['name'], fni['url'])
                fni = self._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._ifeng_detail)
                else:
                    continue
        elif seed['name'] == 'cnfol':
            rsp = demjson.decode(response.body_as_unicode())
            items = rsp.get('list', None)
            if items is not None:
                for it in items:
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], it.get('ContId'))
                    fni['title'] = it.get('Title')
                    fni['url'] = it.get('Url')                    
                    fni['time'] = datetime.datetime.fromtimestamp(float(it.get('CreatedTime3g'))).strftime('%Y-%m-%d %H:%M:%S')
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._cnfol_detail)
                    else:
                        continue
        elif seed['name'] == 'sohu':
            items = demjson.decode(response.body_as_unicode())
            if items is not None:
                for it in items:
                    fni = FinNewsItem()
                    fni['seed'] = seed['name']
                    fni['nid'] = "%s%s"%(seed['name'], it.get('id'))
                    fni['title'] = it.get('title')
                    fni['url'] = "http://www.sohu.com/a/%s_%s" % (it.get('id'), it.get('authorId'))
                    fni['time'] = datetime.datetime.fromtimestamp(float(it.get('publicTime'))/1000).strftime('%Y-%m-%d %H:%M:%S')
                    fni = self._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._sohu_detail)
                    else:
                        continue
        elif seed['name'] == 'cnstock':
            for text in response.css('div.main-content ul.new-list>li:not(.line)'):
                fni = FinNewsItem()
                fni['seed'] = seed['name']
                fni['title'] = text.css('a::text').extract_first().strip()
                fni['url'] = text.css('a::attr(href)').extract_first().strip()
                fni['time'] = text.css('span.time::text').extract_first().strip().replace('[','').replace(']','')
                fni['nid'] = "%s%s"%(seed['name'], fni['url'])
                fni = self._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._cnstock_detail)
                else:
                    continue

    def _hexun_detail(self, response):
        return self._exact_detail("div.art_contextBox", response)

    def _eastmoney_detail(self, response):
        return self._exact_detail("div#ContentBody>p", response)

    def _sina_detail(self, response):
        return self._exact_detail("div#artibody>p", response)

    def _qq_detail(self, response):
        if self._exact_detail("div#Cnt-Main-Article-QQ div.gallery", response, use_auto=False):
            return None
        else:
            return self._exact_detail("div#Cnt-Main-Article-QQ", response)

    def _ifeng_detail(self, response):
        return self._exact_detail("div#main_content", response)

    def _cnfol_detail(self, response):
        return self._exact_detail("div#Content", response)

    def _sohu_detail(self, response):
        return self._exact_detail("article.article", response)

    def _cnstock_detail(self, response):
        return self._exact_detail("div.main-content div.content", response)

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
            return self.detail(response)
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

class FinNewsItem(Item):
    seed = Field()
    nid = Field()
    title = Field()
    url = Field()
    time = Field()
    content = Field()
    def __str__(self):
        return '%s %s %s' % (self['seed'], self['nid'], self['title'])

class FinNewsPipeline(object):
    def open_spider(self, spider):
        mode = 'w+' if spider.full else 'a+'
        self.file = open(spider.out_news_file, mode=mode, encoding='utf-8')
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