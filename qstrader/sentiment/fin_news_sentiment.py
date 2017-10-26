#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import logging.config

import jieba
from gensim import corpora, models

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
from base import AbstractSentiment

class FinNewsSentiment(AbstractSentiment):

    name = "Finance News Sentiment"

    def __init__(self, out_root_path, full, conf, logger=None):
        self.out_root_path = out_root_path
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger()

    def config(self, conf):
        self.topic_conf = conf['topic']
        self.in_news_file = os.path.join(self.out_root_path, conf['in_news_file'])
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_seg_file = os.path.join(self.out_path, conf['out_seg_file'])
        self.out_dic_file = os.path.join(self.out_path, conf['out_dic_file'])
        self.out_d2b_file = os.path.join(self.out_path, conf['out_d2b_file'])
        self.out_tfidf_file = os.path.join(self.out_path, conf['out_tfidf_file'])

    def corpus(self):
        if not os.path.exists(self.in_news_file):
            raise Exception('No input news file found')
        self.corpus_docs = []
        self.corpus_ids = []
        start_time = time.time()
        self.logger.info("start word segment...")
        with open(self.in_news_file, mode='r+', encoding='utf-8') as fi:
            with open(self.out_seg_file, mode='w+', encoding='utf-8') as fo:
                line = fi.readline()
                while line:
                    line = line.split('\t')
                    self.corpus_ids.append(line[0])
                    line = list(jieba.cut(line[4]))
                    self.corpus_docs.append(line)
                    fo.write(" ".join(line))
                    line = fi.readline()
        self.logger.info("end word segment cost %ds" % (time.time() - start_time))
        with open(self.out_id_file,'w') as fo:
            fo.write("\n".join(self.corpus_ids))
        start_time = time.time()
        self.logger.info("start dictionary...")
        self.dictionary = corpora.Dictionary(self.corpus_docs, prune_at=None)
        self.dictionary.save(self.out_dic_file)
        self.logger.info("end dictionary cost %ds" % (time.time() - start_time))
        self.docbow = []
        start_time = time.time()
        self.logger.info("start doc2bow...")
        for doc in self.corpus_docs:
            self.docbow.append(self.dictionary.doc2bow(doc, allow_update=False))
        corpora.MmCorpus.serialize(self.out_d2b_file, self.docbow)
        self.logger.info("end doc2bow cost %ds" % (time.time() - start_time))
        start_time = time.time()
        self.logger.info("start tfidf...")
        self.tfidf_model = models.TfidfModel(self.docbow)
        self.tfidf = self.tfidf_model[self.docbow]
        corpora.MmCorpus.serialize(self.out_tfidf_file, self.tfidf)
        self.logger.info("end tfidf cost %ds" % (time.time() - start_time))

    def train(self):
        start_time = time.time()
        self.logger.info("start train topic model...")
        self.lda_model = models.LdaMulticore(self.tfidf, id2word=self.dictionary, 
            num_topics=self.topic_conf['num_topics'], 
            eval_every=self.topic_conf['eval_every'], batch=False, chunksize=self.topic_conf['chunksize'],
            passes=self.topic_conf['passes'], 
            workers=self.topic_conf['workers'])
        self.logger.info("end train topic model cost %ds" % (time.time() - start_time))

    def load(self):
        raise NotImplementedError("Should implement load()")

