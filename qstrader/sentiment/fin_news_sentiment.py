#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

import jieba

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from base import AbstractSentiment

class FinNewsSentiment(AbstractSentiment):

    name = "Finance News Sentiment"

    def __init__(self, out_root_path, full, conf):
        self.out_root_path = out_root_path
        self.full = full
        self.config(conf)

    def config(self, conf):
        self.in_news_file = os.path.join(self.out_root_path, conf['in_news_file'])
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_seg_file = os.path.join(self.out_path, conf['out_seg_file'])

    def corpus(self):
        if not os.path.exists(self.in_news_file):
            raise Exception('No input news file found')
        with open(self.in_news_file, mode='r+', encoding='utf-8') as fi:      
            with open(self.out_seg_file, mode='w+', encoding='utf-8') as fo:            
                line = fi.readline()
                while line:
                    line = line.split('\t')[4]
                    line = jieba.cut(line)
                    fo.write(" ".join(line))
                    line = fi.readline()

    def train(self):
        raise NotImplementedError("Should implement train()")

    def load(self):
        raise NotImplementedError("Should implement load()")

