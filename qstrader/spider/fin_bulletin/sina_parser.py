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

class SinaParser(object):

    name = 'sina'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        page_num = self.conf['page_num']
        for p in range(max_page, 0, -1):
            url = self.conf['url'] % (page_num, p, int(datetime.datetime.now().timestamp()*1000))
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        rsp = self.spider._jsonp(response.body_as_unicode(), end=-7)
        rsp = demjson.decode(rsp)
        items = rsp.get('result', None)
        items = items.get('data', None) if items is not None else None        
        if items is not None:
            for it in sorted(items, key=lambda x: x['ctime']):                    
                fni = FinNewsItem()
                fni['seed'] = conf['name']
                fni['nid'] = "%s%s"%(conf['name'], it.get('docid'))
                fni['title'] = it.get('title')
                fni['url'] = it.get('url')
                fni['time'] = datetime.datetime.fromtimestamp(float(it.get('ctime'))).strftime('%Y-%m-%d %H:%M:%S')
                fni = self.spider._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                else:
                    continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("div#artibody>p", response)
