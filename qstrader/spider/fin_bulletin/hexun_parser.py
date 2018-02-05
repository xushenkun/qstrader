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

class HexunParser(object):

    name = 'hexun'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        for p in range(max_page, 0, -1):
            url = self.conf['url'] % (p, datetime.datetime.now().strftime('%Y-%m-%d'))
            print(url)
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        rsp = demjson.decode(response.body_as_unicode())
        items = rsp.get('list', None)
        if items is not None:
            current = datetime.datetime.now()
            for it in sorted(items, key=lambda x: x['time']):
                fni = FinNewsItem()
                fni['seed'] = conf['name']
                fni['nid'] = "%s%s"%(conf['name'], it.get('id'))
                fni['title'] = it.get('title')
                fni['url'] = it.get('titleLink')
                year = current.year if current.month >= int(it.get('time')[:2]) else current.year-1
                fni['time'] = "%s-%s"%(year, it.get('time'))
                fni = self.spider._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                else:
                    continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("div.art_contextBox", response)
