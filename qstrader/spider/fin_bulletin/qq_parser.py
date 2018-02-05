#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import logging.config

import scrapy
from scrapy.selector import Selector
import demjson

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from fin_news_spider import FinNewsItem, FinNewsParser

class QqParser(object):

    name = 'qq'

    def __init__(self, spider, conf):
        self.spider = spider
        self.conf = conf

    def start_requests(self):
        max_page = self.conf['full_max_page'] if self.spider.full else self.conf['incr_max_page']
        for p in range(max_page, 0, -1):
            url = self.conf['url'] % (p, int(datetime.datetime.now().timestamp()*1000))
            yield scrapy.Request(url=url, meta={'conf': self.conf}, callback=self._parse_list, headers={'Referer':'http://roll.finance.qq.com/'})

    def _parse_list(self, response):
        conf = response.meta.get('conf')
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
                fni['seed'] = conf['name']
                fni['nid'] = "%s%s"%(conf['name'], href)
                fni['title'] = title
                fni['url'] = href
                year = current.year if current.month >= int(ctime[:2]) else current.year-1
                fni['time'] = "%s-%s"%(year, ctime)
                fni = self.spider._filter_by_title(fni)
                if fni: 
                    yield scrapy.Request(url=fni['url'], meta={'item': fni}, callback=self._parse_detail)
                else:
                    continue

    def _parse_detail(self, response):
        if self.spider._exact_detail("div#Cnt-Main-Article-QQ div.gallery", response, use_auto=False):
            return None
        else:
            return self.spider._exact_detail("div#Cnt-Main-Article-QQ", response)
