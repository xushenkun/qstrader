#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import itertools
import logging.config

import numpy as np
from gensim import corpora, models
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from sentiment.base import AbstractSentiment
from corpus.fin_news_corpus import FinNewsCorpus

class FinNewsSentiment(AbstractSentiment):

    name = "Finance News Sentiment"

    def __init__(self, global_conf, full, conf, logger=None):
        self.out_root_path = global_conf['out_path']
        self.lock_timeout = global_conf['lock_timeout']
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger('sentiment')
        self.corpus = FinNewsCorpus(global_conf, full, global_conf['corpus']['classes'][0])

        self.sentiment_ids = []
        self.lda_model = None    
        self.lda_d2v = None

    def config(self, conf):        
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_tpc_file = os.path.join(self.out_path, conf['out_tpc_file'])
        self.out_d2v_file = os.path.join(self.out_path, conf['out_d2v_file'])
        topic_conf = conf['topic']
        self.num_topics = topic_conf['num_topics']
        self.eval_every = topic_conf['eval_every']
        self.chunksize = topic_conf['chunksize']
        self.passes = topic_conf['passes']
        self.workers = topic_conf['workers']

    def train(self):
        lock = filelock.FileLock(self.out_lock_file)
        with lock.acquire(timeout=self.lock_timeout):
            self.corpus.load()
            start_time = time.time()
            self.logger.info("start train model...")
            if not self.full:
                self.load()
                more_ids = np.setdiff1d(self.corpus.corpus_ids, self.sentiment_ids)            
                if more_ids is not None and len(more_ids)>0:                
                    more_ids, more_tfidf = self._find_tfidf(more_ids)
                    if more_tfidf:
                        self.sentiment_ids.extend(more_ids)
                        self.lda_model.update(more_tfidf)
                        self.lda_model.save(self.out_tpc_file)
                        more_d2v = self.lda_model[more_tfidf]
                        self.lda_d2v = list(itertools.chain(self.lda_d2v, more_d2v))
                        corpora.MmCorpus.serialize(self.out_d2v_file, self.lda_d2v)
            else:
                self.sentiment_ids = self.corpus.corpus_ids
                self.lda_model = models.LdaMulticore(self.corpus.tfidf, id2word=self.corpus.dictionary, 
                    num_topics=self.num_topics, eval_every=self.eval_every, batch=False, chunksize=self.chunksize,
                    passes=self.passes, workers=self.workers)
                self.lda_model.save(self.out_tpc_file)        
                self.lda_d2v = self.lda_model[self.corpus.tfidf]        
                corpora.MmCorpus.serialize(self.out_d2v_file, self.lda_d2v)
            with open(self.out_id_file,'w') as fo:
                fo.write("\n".join(self.sentiment_ids))
            self.logger.info("end train model cost %ds" % (time.time() - start_time))

    def load(self):
        with open(self.out_id_file,'r') as fi:
            line = fi.readline()
            while line:
                self.sentiment_ids.append(line.strip())
                line = fi.readline()
        self.lda_model = models.LdaMulticore.load(self.out_tpc_file, mmap='r')
        self.lda_d2v = corpora.MmCorpus(self.out_d2v_file)

    def _find_tfidf(self, more_ids):
        all_tfidf = self.corpus.tfidf
        more_ids = np.array(more_ids)
        all_ids = np.array(self.corpus.corpus_ids)
        if not self.full:
            rets = np.where(all_ids==more_ids[:,None])
            if rets is None or rets==False or len(rets) < 2:
                return [], []
            more_pos, tfidf_pos = rets[0], rets[1]
            more_ids = more_ids[more_pos]
            more_tfidf = list(self.corpus.tfidf[tfidf_pos])
            return more_ids, more_tfidf
        else:            
            sorter = np.argsort(all_ids)
            rank = np.searchsorted(all_ids, more_ids, sorter=sorter)
            tfidf_pos = sorter[rank]
            return more_ids, list(self.corpus.tfidf[tfidf_pos])