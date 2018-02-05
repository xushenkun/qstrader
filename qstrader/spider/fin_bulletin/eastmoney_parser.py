#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import logging.config

import scrapy

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from fin_news_spider import FinNewsItem, FinNewsParser

class EastmoneyParser(object):

    name = 'eastmoney'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        urls = self.conf['url']
        for p in range(max_page, 0, -1):
            for url in urls:
                url = url % (p)
                yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)   

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        current = datetime.datetime.now()
        for text in response.css('ul#newsListContent>li>div.text'):
            fni = FinNewsItem()
            fni['seed'] = conf['name']
            fni['title'] = text.css('p.title>a::text').extract_first().strip()
            fni['url'] = text.css('p.title>a::attr(href)').extract_first().strip()
            fni['time'] = text.css('p.time::text').extract_first().strip()
            fni['time'] = fni['time'].replace('月','-').replace('日','')
            year = current.year if current.month >= int(fni['time'][:2]) else current.year-1
            fni['time'] = "%s-%s"%(year, fni['time'])
            fni['nid'] = "%s%s"%(conf['name'], fni['url'])
            fni = self.spider._filter_by_title(fni)
            if fni: 
                yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
            else:
                continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("div#ContentBody>p", response)
