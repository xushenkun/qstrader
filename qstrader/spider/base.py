#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, yaml
import logging.config
from abc import ABCMeta, abstractmethod

from scrapy                 import signals, Spider
from pydispatch             import dispatcher
from scrapy.crawler         import CrawlerProcess
from scrapy.utils.project   import get_project_settings
from scrapy.utils.log import configure_logging

from newspaper import fulltext as fulltext3rd

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')

class AbstractSpider(Spider):

    __metaclass__ = ABCMeta

    @abstractmethod
    def config(self, conf):
        raise NotImplementedError("Should implement config(conf)")

    def fulltext(self, response):
        item = response.meta.get('item')
        if item is not None:
            #response.css('div.art_contextBox')
            html = response.body_as_unicode()
            if html:
                try:
                    text = fulltext3rd(html, language='zh')
                    text = text.split()
                    text = [line.strip() for line in text]
                    content = "".join([line for line in text if line])
                    if content:
                        item['content'] = content
                        return item
                except Exception as e:
                    self.an_logger.error("full text error for %s" % item.get('url'))
        return None

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
        with open(config_path, 'r', encoding='utf-8') as fi:
            self.global_conf = yaml.load(fi)
            self.bot_name = self.global_conf['spider']['bot_name']
            self.user_agent = self.global_conf['spider']['user_agent']
            self.spider_confs = self.global_conf['spider']['classes']      
            self.log_conf_path = self.global_conf['log']['config_path']
            with open(self.log_conf_path, 'r', encoding='utf-8') as fi:
                log_settings = yaml.load(fi)
                #configure_logging(settings=log_settings, install_root_handler=False)
                logging.config.dictConfig(log_settings)
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
                self.process.crawl(spider_conf['name'], global_conf=self.global_conf, full=self.full, conf=spider_conf, logger=self.logger)
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