#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import itertools
import logging.config
from collections import defaultdict

import jieba
jieba.load_userdict(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "userdict.txt")
import jieba.posseg as pseg
from gensim import corpora, models
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from util.common import get_stop_word, is_number, is_punctuation
from corpus.base import AbstractCorpus
from data.tushare_data import TushareData

TOPIC_KEYWORD_POS_TAGS = ['an', 'i', 'j', 'l', 'n', 'nr', 'nrt', 'nrfg', 'ns', 'nt', 'nz', 'vn', 'eng']
SENTENCE_DELIMITERS = ['?', '!', ';', '？', '！', '。', '；', '……', '…', '\n']
KEYWORD_POS_TAGS = ['an', 'i', 'j', 'l', 'n', 'nr', 'nrt', 'nrfg', 'ns', 'nt', 'nz', 't', 'v', 'vd', 'vn', 'eng']

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
        for word in self.tushare.get_stock_names():
            jieba.add_word(word, freq=11111111)

        self.corpus_ids = [] 
        self.corpus_docs = []
        self.corpus_pos_docs = []
        self.dictionary = None     
        self.docbow = []
        self.tfidf = None

    def config(self, conf):
        self.in_news_file = os.path.join(self.out_root_path, conf['in_news_file'])
        self.days_ago = conf['days_ago']
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_seg_file = os.path.join(self.out_path, conf['out_seg_file'])
        self.out_pos_file = os.path.join(self.out_path, conf['out_pos_file'])
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
            with open(self.out_id_file, 'w', encoding='utf-8') as fo:
                fo.write("\n".join(self.corpus_ids))
            self.logger.info("end generate cost %ds" % (time.time() - start_time))

    def load(self):
        start_time = time.time()
        self.logger.info("start load...")
        with open(self.out_id_file,'r', encoding='utf-8') as fi:
            line = fi.readline()
            while line:
                self.corpus_ids.append(line.strip())
                line = fi.readline()
        with open(self.out_seg_file,'r', encoding='utf-8') as fi:
            line = fi.readline()
            while line:
                self.corpus_docs.append(line.strip().split('\t'))
                line = fi.readline()
        with open(self.out_pos_file,'r', encoding='utf-8') as fi:
            line = fi.readline()
            while line:
                self.corpus_pos_docs.append(line.strip().split('\t'))
                line = fi.readline()
        self.dictionary = corpora.Dictionary.load(self.out_dic_file, mmap='r')
        self.docbow = corpora.MmCorpus(self.out_d2b_file)
        self.tfidf = corpora.MmCorpus(self.out_tfidf_file)
        self.logger.info("end load cost %ds" % (time.time() - start_time))

    def rank_keyword(self, pos_words, win_size=10, allow_pos=TOPIC_KEYWORD_POS_TAGS, check_oov=True, max_num=5, filter_words=None):
        pos_words = pos_words.split(' ')
        length = len(pos_words)
        import networkx as nx
        dg = nx.DiGraph()
        words = []
        for i, pos_word in enumerate(pos_words):
            pos_word = pos_word.split('/')
            words.append(pos_word[0])
            if (not check_oov or pos_word[0] in self.dictionary.token2id) and (allow_pos is None or pos_word[1] in allow_pos):
                for j in range(i+1, i+win_size):
                    if j >= length: break
                    pw2 = pos_words[j].split('/')
                    if (not check_oov or pw2[0] in self.dictionary.token2id) and (allow_pos is None or pw2[1] in allow_pos):
                        weight = dg.get_edge_data(pos_word[0], pw2[0], None)
                        if weight is None:
                            dg.add_edge(pos_word[0], pw2[0], w=1)
                        else:
                            dg.add_edge(pos_word[0], pw2[0], w=weight["w"]+1)
        pr = nx.pagerank(dg, alpha=0.85, weight="w")
        items = sorted(pr.items(), key=lambda x: x[1], reverse=True)
        keyword_scores = list(filter(lambda x: x[0] not in filter_words, items))[:max_num] if filter_words is not None else items[:max_num]
        #self.logger.info(" ".join(words))
        #self.logger.info(keyword_scores)
        #name = input("Continue ?")
        return keyword_scores

    def pos_word_filter(self, pos_words, allow_pos=TOPIC_KEYWORD_POS_TAGS, in_dictionary=True):
        all_words, filter_words = [], []
        pos_words = pos_words.split('\t')
        for pos_word in pos_words:
            pos_word = pos_word.split('/')
            if not in_dictionary or pos_word[0] in self.dictionary.token2id:
                all_words.append(pos_word[0])
                if allow_pos is None or pos_word[1] in allow_pos:
                    filter_words.append(pos_word[0])
        return all_words, filter_words

    def _segment(self):
        more_corpus_docs = []
        start_time = time.time()
        self.logger.info("start word segment...")
        if self.days_ago >= 0:
            today = datetime.datetime.now()
            today = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
            before = (today + datetime.timedelta(days=-1*self.days_ago)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            before = ''
        with open(self.in_news_file, mode='r', encoding='utf-8') as fi:
            seg_file_mode = 'w' if self.full else 'a'
            pos_file_mode = 'w' if self.full else 'a'
            with open(self.out_seg_file, mode=seg_file_mode, encoding='utf-8') as seg_fo:
                with open(self.out_pos_file, mode=pos_file_mode, encoding='utf-8') as pos_fo:
                    line = fi.readline()
                    while line:
                        line = line.strip().split('\t')
                        if self.full or line[1] not in self.corpus_ids:
                            if line[4] >= before:
                                self.corpus_ids.append(line[1])
                                pos_words = [wp for wp in pseg.cut(line[5])]
                                pos_content = ["%s/%s"%(wp.word, wp.flag) for wp in pos_words]
                                content = [wp.word for wp in pos_words]
                                more_corpus_docs.append(content)
                                seg_fo.write("%s\t%s\t%s\n" % (line[1], line[2], " ".join(content)))
                                pos_fo.write("%s\t%s\t%s\n" % (line[1], line[2], " ".join(pos_content)))
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
        self.logger.info("dictionary length is %d" % len(self.dictionary))
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
        self.logger.info("tfidf length is %d" % len(self.tfidf))
        self.logger.info("end tfidf cost %ds" % (time.time() - start_time))