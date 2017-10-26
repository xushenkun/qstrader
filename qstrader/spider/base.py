#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import logging.config
from abc import ABCMeta, abstractmethod
import yaml

from scrapy                 import signals, Spider
from pydispatch             import dispatcher
from scrapy.crawler         import CrawlerProcess
from scrapy.utils.project   import get_project_settings

from newspaper import fulltext

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')

class AbstractSpider(Spider):

    __metaclass__ = ABCMeta

    @abstractmethod
    def config(self, conf):
        raise NotImplementedError("Should implement config(conf)")

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

class Spiders(object):
    def __init__(self, full, conf, active_ids=None, signal=signals.item_passed, slot=None):
        self.full = full      
        self.active_ids = active_ids
        self.signal = signal
        self.slot = slot
        self.process = None
        self.logger = None
        self.config(conf)

    def config(self, config_path):
        with open(config_path) as fi:
            conf = yaml.load(fi)
            self.out_path = conf['out_path']
            self.bot_name = conf['spider']['bot_name']
            self.user_agent = conf['spider']['user_agent']
            self.spider_confs = conf['spider']['classes']      
            self.log_conf_path = conf['log']['config_path']
            with open(self.log_conf_path, 'r') as fi:
                logging.config.dictConfig(yaml.load(fi))
                self.logger = logging.getLogger('spider')      

    def start(self):
        self.logger.info("start spider...")
        settings = {"BOT_NAME": self.bot_name, "USER_AGENT": self.user_agent, "SPIDER_MODULES": ['spider']}
        #settings = get_project_settings()
        self.process = CrawlerProcess(settings)
        if (self.slot is not None):
            dispatcher.connect(self.slot, self.signal)
        for spider_conf in self.spider_confs:
            if self.active_ids is None or spider_conf['id'] in self.active_ids:
                self.process.crawl(spider_conf['name'], out_root_path=self.out_path, full=self.full, conf=spider_conf, logger=self.logger)
        self.process.start()
        self.logger.info("end spider")

    def stop(self):
        if self.process:
            self.process.stop()
            self.process = None

if __name__ == '__main__':
    assert len(sys.argv) == 3, "Spider script should have enought arguments like: python base.py full|incr config_path"    
    full = True if sys.argv[1].lower() == 'full' else False
    config_path = sys.argv[2]
    spiders = Spiders(full=full, conf=config_path, active_ids=None)
    spiders.start()