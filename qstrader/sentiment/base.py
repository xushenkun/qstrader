#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, yaml
import logging.config
from abc import ABCMeta, abstractmethod

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from util.common import load_classes

class AbstractSentiment(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def config(self, conf):
        raise NotImplementedError("Should implement config(conf)")

    @abstractmethod
    def train(self):
        raise NotImplementedError("Should implement train()")

    @abstractmethod
    def load(self):
        raise NotImplementedError("Should implement load()")

class Sentiments(object):
    def __init__(self, full, conf, active_ids=None):
        self.full = full    
        self.active_ids = active_ids
        self.sentiments = []     
        self.logger = None   
        self.config(conf)

    def config(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as fi:
            self.global_conf = yaml.load(fi)
            self.log_conf_path = self.global_conf['log']['config_path']
            with open(self.log_conf_path, 'r', encoding='utf-8') as fi:
                logging.config.dictConfig(yaml.load(fi))
                self.logger = logging.getLogger('sentiment')  
            self.sentiment_confs = self.global_conf['sentiment']['classes']
            self.sentiment_classes = load_classes('sentiment', AbstractSentiment)
            for sc in self.sentiment_confs:
                if (self.active_ids is None or sc['id'] in self.active_ids) and self.sentiment_classes.get(sc['name'], None):
                    self.sentiments.append(self.sentiment_classes[sc['name']](self.global_conf, self.full, sc, self.logger))

    def start(self):
        self.logger.info("start sentiment...")
        for s in self.sentiments:
            s.train()
        self.logger.info("end sentiment")

if __name__ == '__main__':
    assert len(sys.argv) == 3, "Sentiment script should have enought arguments like: python base.py full|incr config_path"
    full = True if sys.argv[1].lower() == 'full' else False
    config_path = sys.argv[2]
    sentiments = Sentiments(full=full, conf=config_path, active_ids=None)
    sentiments.start()        