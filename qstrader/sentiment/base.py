#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import yaml, six, inspect
import logging.config
from importlib import import_module
from abc import ABCMeta, abstractmethod
from pkgutil import iter_modules

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')

class AbstractSentiment(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def config(self, conf):
        raise NotImplementedError("Should implement config(conf)")

    @abstractmethod
    def corpus(self):
        raise NotImplementedError("Should implement corpus()")

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
        with open(config_path) as fi:
            self.global_conf = yaml.load(fi)
            self.log_conf_path = self.global_conf['log']['config_path']
            with open(self.log_conf_path, 'r') as fi:
                logging.config.dictConfig(yaml.load(fi))
                self.logger = logging.getLogger('sentiment')  
            self.sentiment_confs = self.global_conf['sentiment']['classes']
            self.sentiment_classes = self._load_sentiment_classes()
            for sc in self.sentiment_confs:
                if (self.active_ids is None or sc['id'] in self.active_ids) and self.sentiment_classes.get(sc['name'], None):
                    self.sentiments.append(self.sentiment_classes[sc['name']](self.global_conf, self.full, sc, self.logger))

    def start(self):
        for s in self.sentiments:
            s.corpus()
            s.train()

    def _load_sentiment_classes(self):
        classes = {}
        mods = self._walk_modules('sentiment')
        for mod in mods:
            for obj in six.itervalues(vars(mod)):
                if inspect.isclass(obj) and obj.__module__ == mod.__name__ and getattr(obj, 'name', None):#and issubclass(obj, AbstractSentiment):# 
                    classes[obj.name] = obj
        return classes

    def _walk_modules(self, path):
        mods = []
        mod = import_module(path)
        mods.append(mod)
        if hasattr(mod, '__path__'):
            for _, subpath, ispkg in iter_modules(mod.__path__):
                fullpath = path + '.' + subpath
                if ispkg:
                    mods += self._walk_modules(fullpath)
                else:
                    submod = import_module(fullpath)
                    mods.append(submod)
        return mods

if __name__ == '__main__':
    assert len(sys.argv) == 3, "Sentiment script should have enought arguments like: python base.py full|incr config_path"
    full = True if sys.argv[1].lower() == 'full' else False
    config_path = sys.argv[2]
    sentiments = Sentiments(full=full, conf=config_path, active_ids=None)
    sentiments.start()        