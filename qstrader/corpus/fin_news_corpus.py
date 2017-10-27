#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import itertools
import logging.config

import jieba
from gensim import corpora, models
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from util.common import get_stop_word, is_number, is_punctuation
from corpus.base import AbstractCorpus
from data.tushare_data import TushareData

class FinNewsCorpus(AbstractCorpus):

    name = "Finance News Corpus"

    def __init__(self, global_conf, full, conf, logger=None):
        self.out_root_path = global_conf['out_path']
        self.stopword_file = global_conf['stopword_file']
        self.lock_timeout = global_conf['lock_timeout']
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger('corpus')
        self.tushare = TushareData(global_conf, self.full, global_conf['data']['tushare'])
        self.stopwords = get_stop_word(self.stopword_file)

        self.corpus_ids = []        
        self.dictionary = None     
        self.docbow = []
        self.tfidf = None

    def config(self, conf):
        self.in_news_file = os.path.join(self.out_root_path, conf['in_news_file'])
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_seg_file = os.path.join(self.out_path, conf['out_seg_file'])
        self.out_dic_file = os.path.join(self.out_path, conf['out_dic_file'])
        self.out_d2b_file = os.path.join(self.out_path, conf['out_d2b_file'])
        self.out_tfidf_file = os.path.join(self.out_path, conf['out_tfidf_file'])
        self.no_below = conf['dictionary']['no_below']
        self.no_above = conf['dictionary']['no_above']
        self.keep_n = conf['dictionary']['keep_n']

    def generate(self):
        lock = filelock.FileLock(self.out_lock_file)
        with lock.acquire(timeout=self.lock_timeout):
            start_time = time.time()
            self.logger.info("start generate...")
            if not os.path.exists(self.in_news_file):
                raise Exception('Input file not found')        
            if not self.full:
                self.load()
            more_corpus_docs = self._segment()
            if more_corpus_docs:
                if self.full:
                    self._dictionary(more_corpus_docs)
                self._doc2bow(more_corpus_docs)
                self._tfidf()
            with open(self.out_id_file,'w') as fo:
                fo.write("\n".join(self.corpus_ids))
            self.logger.info("end generate cost %ds" % (time.time() - start_time))

    def load(self):
        start_time = time.time()
        self.logger.info("start load...")
        with open(self.out_id_file,'r') as fi:
            line = fi.readline()
            while line:
                self.corpus_ids.append(line.strip())
                line = fi.readline()
        self.dictionary = corpora.Dictionary.load(self.out_dic_file, mmap='r')
        self.docbow = corpora.MmCorpus(self.out_d2b_file)
        self.tfidf = corpora.MmCorpus(self.out_tfidf_file)
        self.logger.info("end load cost %ds" % (time.time() - start_time))

    def _segment(self):
        more_corpus_docs = []
        start_time = time.time()
        self.logger.info("start word segment...")
        with open(self.in_news_file, mode='r', encoding='utf-8') as fi:
            seg_file_mode = 'w' if self.full else 'a'
            with open(self.out_seg_file, mode=seg_file_mode, encoding='utf-8') as fo:
                line = fi.readline()
                while line:
                    line = line.split('\t')
                    if self.full or line[0] not in self.corpus_ids:
                        self.corpus_ids.append(line[0])
                        line = list(jieba.cut(line[4]))
                        more_corpus_docs.append(line)
                        fo.write(" ".join(line))
                    line = fi.readline()
        self.logger.info("end word segment cost %ds" % (time.time() - start_time))        
        return more_corpus_docs

    def _dictionary(self, corpus_docs):
        start_time = time.time()
        self.logger.info("start dictionary...")
        self.dictionary = corpora.Dictionary(corpus_docs, prune_at=None)
        self.dictionary.filter_extremes(no_below=self.no_below, no_above=self.no_above, keep_n=self.keep_n)
        del_num_ids = [self.dictionary.token2id[word] for word in self.dictionary.values() if (not self.tushare.is_stock_code(word) and is_number(word)) or is_punctuation(word) or word in self.stopwords]
        self.dictionary.filter_tokens(del_num_ids)
        self.dictionary.compactify()
        self.dictionary.save(self.out_dic_file)
        self.logger.info("end dictionary cost %ds" % (time.time() - start_time))

    def _doc2bow(self, corpus_docs):
        start_time = time.time()
        self.logger.info("start doc2bow...")
        new_docbow = []
        for doc in corpus_docs:
            new_docbow.append(self.dictionary.doc2bow(doc, allow_update=False))
        self.docbow = list(itertools.chain(self.docbow, new_docbow))
        corpora.MmCorpus.serialize(self.out_d2b_file, self.docbow)
        self.logger.info("end doc2bow cost %ds" % (time.time() - start_time))

    def _tfidf(self):
        start_time = time.time()
        self.logger.info("start tfidf...")
        self.tfidf_model = models.TfidfModel(self.docbow)
        self.tfidf = self.tfidf_model[self.docbow]
        corpora.MmCorpus.serialize(self.out_tfidf_file, self.tfidf)
        self.logger.info("end tfidf cost %ds" % (time.time() - start_time))