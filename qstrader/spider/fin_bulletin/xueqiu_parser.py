#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import logging.config

import scrapy
import demjson

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from fin_news_spider import FinNewsItem, FinNewsParser

class XueqiuParser(object):

    name = 'xueqiu'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        url = 'https://xueqiu.com/' #for cookie
        yield scrapy.Request(url=url, meta={'conf': self.conf, 'max_page': max_page, 'curr_page': -1}, callback=self._parse_list)  

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        if response.meta.get('curr_page') == -1:
            url = conf['url'] % (conf['page_num'], -1)
            yield scrapy.Request(url=url, meta={'conf': conf, 'max_page': response.meta.get('max_page'), 'curr_page': 0}, callback=self._parse_list)
        else:        
            rsp = demjson.decode(response.body_as_unicode())        
            if response.meta.get('curr_page') < response.meta.get('max_page'):
                if rsp.get('next_max_id', 1) > 1:
                    url = conf['url'] % (conf['page_num'], rsp.get('next_max_id', 1))
                    yield scrapy.Request(url=url, meta={'conf': conf, 'max_page': response.meta.get('max_page'), 'curr_page': response.meta.get('curr_page')+1}, callback=self._parse_list)                
            items = rsp.get('list', None)
            if items is not None:
                for it in items:
                    fni = FinNewsItem()
                    fni['seed'] = conf['name']                    
                    data = demjson.decode(it.get('data'))
                    fni['nid'] = "%s%s"%(conf['name'], data.get('id'))
                    fni['title'] = data.get('title')
                    fni['url'] = "https://xueqiu.com%s" % data.get('target')
                    fni['time'] = datetime.datetime.fromtimestamp(float(data.get('created_at'))/1000).strftime('%Y-%m-%d %H:%M:%S')
                    fni = self.spider._filter_by_title(fni)
                    if fni: 
                        yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                    else:
                        continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("article div", response)
