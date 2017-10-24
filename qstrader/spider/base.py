#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod

from scrapy                 import signals, Spider
from pydispatch             import dispatcher
from scrapy.crawler         import CrawlerProcess
from scrapy.utils.project   import get_project_settings

from newspaper import fulltext

class AbstractSpider(Spider):
    """
    The AbstractSpider abstract class modifies
    the quantity (or not) of any share transacted
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def other(self):
        raise NotImplementedError("Should implement other()")

    def detail(self, response):
        item = response.meta.get('item')
        if item is not None:
            #response.css('div.art_contextBox')
            html = response.body_as_unicode()
            text = fulltext(html, language='zh')
            text = text.split()
            text = [line.strip() for line in text]
            item['content'] = "".join([line for line in text if line])
            return item

class Spiders(AbstractSpider):
    """
    Spiders is a collection of spider
    """
    def __init__(self, spiders, data_path, full, signal=signals.item_passed, slot=None):
        self.spiders = spiders
        self.data_path = data_path
        self.full = full
        self.signal = signal
        self.slot = slot
        self.process = None

    def start(self):
        settings = {"BOT_NAME": "QSpiders", "USER_AGENT": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36"}
        #settings = get_project_settings()
        self.process = CrawlerProcess(settings)
        if (self.slot is not None):
            dispatcher.connect(self.slot, self.signal)
        for spider in self.spiders:
            self.process.crawl(spider, data_path=self.data_path, full=self.full)
        self.process.start()

    def stop(self):
        if self.process:
            self.process.stop()
            self.process = None