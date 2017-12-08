#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging.config

import scrapy
import demjson

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from fin_news_spider import FinNewsItem, FinNewsParser

class IfengParser(object):

    name = 'ifeng'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        for p in range(max_page, 0, -1):
            url = self.conf['url'] % (p)
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list)

    def _parse_list(self, response):
        conf = response.meta.get('conf')
        for text in response.css('div.box_mid div.box_list_word'):
            fni = FinNewsItem()
            fni['seed'] = conf['name']
            fni['title'] = text.css('h2>a::text').extract_first().strip()
            fni['url'] = text.css('h2>a::attr(href)').extract_first().strip()
            fni['time'] = text.css('div.keywords>a::text').extract_first().strip()
            fni['nid'] = "%s%s"%(conf['name'], fni['url'])
            fni = self.spider._filter_by_title(fni)
            if fni: 
                yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
            else:
                continue

    def _parse_detail(self, response):
        return self.spider._exact_detail("div#main_content", response)
