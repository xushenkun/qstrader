#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging.config

import scrapy
import demjson

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from fin_news_spider import FinNewsItem, FinNewsParser

class NeteaseParser(object):

    name = 'netease'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        page_num = self.conf['page_num']
        for p in range(max_page, -1, -1):
            url = self.conf['url'] % (p*page_num, page_num)
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        rsp = self.spider._jsonp(response.body_as_unicode())
        rsp = demjson.decode(rsp)
        items = rsp.get('BA8EE5GMwangning', None)            
        if items is not None:
            for it in sorted(items, key=lambda x: x['ptime']):                    
                fni = FinNewsItem()
                fni['seed'] = conf['name']
                fni['nid'] = "%s%s"%(conf['name'], it.get('docid'))
                fni['title'] = it.get('title')
                fni['url'] = it.get('url')
                fni['time'] = it.get('ptime')
                fni = self.spider._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                else:
                    continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("article div#content", response)
