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

class CnfolParser(object):

    name = 'cnfol'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        page_num = self.conf['page_num']
        now = int(datetime.datetime.now().timestamp()*1000)
        for p in range(max_page, 0, -1):
            url = self.conf['url'] % (now/1000-14400, now/1000, page_num, p, now)
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        rsp = demjson.decode(response.body_as_unicode())
        items = rsp.get('list', None)
        if items is not None:
            for it in items:
                fni = FinNewsItem()
                fni['seed'] = conf['name']
                fni['nid'] = "%s%s"%(conf['name'], it.get('ContId'))
                fni['title'] = it.get('Title')
                fni['url'] = it.get('Url')                    
                fni['time'] = datetime.datetime.fromtimestamp(float(it.get('CreatedTime3g'))).strftime('%Y-%m-%d %H:%M:%S')
                fni = self.spider._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                else:
                    continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("div#Content", response)
