#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import logging.config

import jieba
from gensim import corpora, models

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '.')
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from base import AbstractSentiment
from data.tushare_data import TushareData

class FinNewsSentiment(AbstractSentiment):

    name = "Finance News Sentiment"

    def __init__(self, global_conf, full, conf, logger=None):
        self.out_root_path = global_conf['out_path']
        self.stopword_file = global_conf['stopword_file']
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger('sentiment')
        self.tushare = TushareData(global_conf, full, global_conf['data']['tushare'])

    def config(self, conf):
        self.topic_conf = conf['topic']
        self.in_news_file = os.path.join(self.out_root_path, conf['in_news_file'])
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_seg_file = os.path.join(self.out_path, conf['out_seg_file'])
        self.out_dic_file = os.path.join(self.out_path, conf['out_dic_file'])
        self.out_d2b_file = os.path.join(self.out_path, conf['out_d2b_file'])
        self.out_tfidf_file = os.path.join(self.out_path, conf['out_tfidf_file'])
        self.out_d2v_file = os.path.join(self.out_path, conf['out_d2v_file'])

        self.stopwords = []
        with open(self.stopword_file, mode='r', encoding='utf-8') as fi:
            line = fi.readline()
            while line:
                self.stopwords.append(line.strip())
                line = fi.readline()

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
        del_num_ids = [self.dictionary.token2id[word] for word in self.dictionary.values() if self._is_number(word) or word in self.stopwords]
        self.dictionary.filter_tokens(del_num_ids)
        self.dictionary.compactify()
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
        self.lda_model.save(self.topic_conf['out_model_file'])
        self.logger.info("end train topic model cost %ds" % (time.time() - start_time))
        start_time = time.time()
        self.logger.info("start doc topic...")
        self.lda_d2v = self.lda_model[self.tfidf]
        corpora.MmCorpus.serialize(self.out_d2v_file, self.lda_d2v)
        self.logger.info("end doc topic cost %ds" % (time.time() - start_time))

    def load(self):
        raise NotImplementedError("Should implement load()")

    def _is_number(self, s):
        if self.tushare.is_stock_code(s):
            return False
        try:
            float(s)
            return True
        except ValueError:
            pass 
        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass
        return False
        