#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, yaml
import logging.config
from abc import ABCMeta, abstractmethod

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from util.common import load_classes

class AbstractCorpus(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def config(self, conf):
        raise NotImplementedError("Should implement config(conf)")

    @abstractmethod
    def generate(self):
        raise NotImplementedError("Should implement generate()")

    @abstractmethod
    def load(self):
        raise NotImplementedError("Should implement load()")

class Corpuses(object):
    def __init__(self, full, conf, active_ids=None):
        self.full = full    
        self.active_ids = active_ids
        self.corpuses = []     
        self.logger = None   
        self.config(conf)

    def config(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as fi:
            self.global_conf = yaml.load(fi)
            self.log_conf_path = self.global_conf['log']['config_path']
            with open(self.log_conf_path, 'r', encoding='utf-8') as fi:
                logging.config.dictConfig(yaml.load(fi))
                self.logger = logging.getLogger('corpus')  
            self.corpus_confs = self.global_conf['corpus']['classes']
            self.corpus_classes = load_classes('corpus', AbstractCorpus)
            for cc in self.corpus_confs:
                if (self.active_ids is None or cc['id'] in self.active_ids) and self.corpus_classes.get(cc['name'], None):
                    self.corpuses.append(self.corpus_classes[cc['name']](self.global_conf, self.full, cc, self.logger))

    def start(self):
        self.logger.info("start corpus...")
        for s in self.corpuses:
            s.generate()
        self.logger.info("end corpus...")

if __name__ == '__main__':
    assert len(sys.argv) == 3, "Corpus script should have enought arguments like: python base.py full|incr config_path"
    full = True if sys.argv[1].lower() == 'full' else False
    config_path = sys.argv[2]
    corpuses = Corpuses(full=full, conf=config_path, active_ids=None)
    corpuses.start()        