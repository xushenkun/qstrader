#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import itertools
import logging.config

import numpy as np
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as sch
from sklearn.manifold import TSNE
from sklearn.decomposition import NMF
from sklearn.preprocessing import normalize
from sklearn.feature_extraction.text import CountVectorizer
from gensim import corpora, models, matutils
import filelock

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')
from sentiment.base import AbstractSentiment
from corpus.fin_news_corpus import FinNewsCorpus
from util.common import get_filter_keyword
from data.tushare_data import TushareData

class FinNewsSentiment(AbstractSentiment):

    name = "Finance News Sentiment"

    def __init__(self, global_conf, full, conf, logger=None):
        self.out_root_path = global_conf['out_path']
        self.lock_timeout = global_conf['lock_timeout']
        self.keyword_filter_file = global_conf['keyword_filter_file']
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger('sentiment')
        self.tushare = TushareData(global_conf, self.full, global_conf['data']['tushare'])
        self.corpus = FinNewsCorpus(global_conf, self.full, global_conf['corpus']['classes'][0])
        self.filter_keywords = get_filter_keyword(self.keyword_filter_file)

        self.sentiment_ids = []
        self.lda_model = None
        self.lda_d2v = None

    def config(self, conf):        
        self.out_path = os.path.join(self.out_root_path, conf['out_folder'])
        self.out_lock_file = os.path.join(self.out_path, conf['out_lock_file'])
        self.out_id_file = os.path.join(self.out_path, conf['out_id_file'])
        self.out_tpc_file = os.path.join(self.out_path, conf['out_tpc_file'])
        self.out_d2v_file = os.path.join(self.out_path, conf['out_d2v_file'])
        self.topic_conf = conf['topic']
        self.cluster_conf = conf['cluster']
        self.keyword_conf = conf['keyword']
        self.textrank_conf = conf['text_rank']
        self.nmf_conf = conf['nmf']
        self.paint = conf['paint']
        self.d2c_fig_file = os.path.join(self.out_path, self.cluster_conf['d2c_fig_file'])
        self.d2c_file = os.path.join(self.out_path, self.cluster_conf['d2c_file'])
        self.t2c_fig_file = os.path.join(self.out_path, self.cluster_conf['t2c_fig_file'])
        self.t2c_file = os.path.join(self.out_path, self.cluster_conf['t2c_file'])

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
                        self.lda_d2v = np.array(corpora.MmCorpus(self.out_d2v_file))
            else:
                self.sentiment_ids = self.corpus.corpus_ids
                self.lda_model = models.LdaMulticore(self.corpus.tfidf, id2word=self.corpus.dictionary, 
                    num_topics=self.topic_conf['num_topics'], eval_every=self.topic_conf['eval_every'], batch=False, chunksize=self.topic_conf['chunksize'], passes=self.topic_conf['passes'], workers=self.topic_conf['workers'])
                self.lda_model.save(self.out_tpc_file)
                self.lda_d2v = self.lda_model[self.corpus.tfidf]
                corpora.MmCorpus.serialize(self.out_d2v_file, self.lda_d2v)
                self.lda_d2v = np.array(corpora.MmCorpus(self.out_d2v_file))
            self.logger.info("doc total number is [%s]" % len(self.lda_d2v))
            with open(self.out_id_file,'w') as fo:
                fo.write("\n".join(self.sentiment_ids))
            self._get_topic_coherence()
            #t2c = self._get_topic_clusters(paint=self.paint)
            d2c = self._get_doc_clusters(paint=self.paint)
            pos_docs, d2c_idx, tfidf_idx = self._get_top_cluster_docs(d2c)
            keywords = self._keywords_by_doc_textrank()
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)
            keywords = self._keywords_by_doc_textrank(pos_docs)
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)
            keywords = self._keywords_by_doc_tfidf()
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)
            keywords = self._keywords_by_doc_tfidf(tfidf_idx)
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)
            keywords = self._keywords_by_doc_topic()
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)
            keywords = self._keywords_by_doc_topic(d2c_idx)
            patterns = self._get_topic_pattern(keywords)
            stocks = self._get_neighbor_stock(patterns)

            self.logger.info("end train model cost %ds" % (time.time() - start_time))

    def load(self):
        with open(self.out_id_file,'r', encoding="utf-8") as fi:
            line = fi.readline()
            while line:
                self.sentiment_ids.append(line.strip())
                line = fi.readline()
        self.lda_model = models.LdaMulticore.load(self.out_tpc_file, mmap='r')
        self.lda_d2v = np.array(corpora.MmCorpus(self.out_d2v_file))

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

    def _get_topic_coherence(self):
        start_time = time.time()
        self.logger.info("start make topic coherence...")
        texts = [d[2].split(' ') for d in self.corpus.corpus_pos_docs]
        texts = [[wp.split('/')[0] for wp in wps] for wps in texts]
        coherence_scores = self.lda_model.top_topics(corpus=self.corpus.tfidf, texts=texts, dictionary=self.corpus.dictionary, coherence='c_v', topn=10)
        coherence_scores = sorted(coherence_scores, key=lambda x: x[1], reverse=True)
        for i, cs in enumerate(coherence_scores[:5]):
            self.logger.info("topic %d is %s" % (i+1, cs[0]))
        self.logger.info("topic coherence is %s" % np.mean([cs[1] for cs in coherence_scores]))
        self.logger.info("end make topic coherence cost %ds" % (time.time() - start_time))

    def _get_doc_clusters(self, paint=False):
        start_time = time.time()
        self.logger.info("start make doc cluster...")
        full_d2v = np.empty(shape=(len(self.lda_d2v), self.topic_conf['num_topics']))
        for i, v in enumerate(self.lda_d2v):
            v = matutils.unitvec(matutils.sparse2full(v, self.topic_conf['num_topics']), norm='l1')
            full_d2v[i] = v
        dist_matrix = sch.distance.pdist(full_d2v, 'euclidean')
        link_matrix = sch.linkage(dist_matrix, method='average')
        cophenet, cophenet_dist = sch.cophenet(link_matrix, dist_matrix)
        self.logger.info("doc cluster cophenet is [%s]" % cophenet)
        self.num_doc_clusters = len(self.lda_d2v) // self.cluster_conf['num_clusters_factor']
        self.logger.info("doc cluster number is [%d]" % self.num_doc_clusters)
        sch_d2c = None
        if self.num_doc_clusters < 2:
            self.logger.error("too small doc cluster number")
        else:        
            sch_d2c= sch.fcluster(link_matrix, t=self.num_doc_clusters, criterion='maxclust')
            with open(self.d2c_file, "w") as fo:
                fo.write("\n".join(map(str, sch_d2c)))
            if paint:
                self._paint(full_d2v, sch_d2c, link_matrix)
        self.logger.info("end make doc cluster cost %ds" % (time.time() - start_time))
        return sch_d2c

    def _get_topic_clusters(self, paint=False):
        start_time = time.time()
        self.logger.info("start make topic cluster...") 
        full_t2v = np.empty(shape=(self.topic_conf['num_topics'], len(self.corpus.dictionary)))
        lda_topics = self.lda_model.show_topics(num_topics=self.topic_conf['num_topics'], num_words=self.nmf_conf['num_words'], formatted=False)
        for tid, topic in lda_topics:
            for word, score in topic:
                full_t2v[tid][self.corpus.dictionary.token2id[word]] = score
        full_t2v = normalize(full_t2v, norm='l1', axis=1)
        self.logger.info("topic cluster nmf training...")
        nmf_model = NMF(n_components=self.nmf_conf['num_comps'], init='nndsvd');
        topic_comps = nmf_model.fit_transform(full_t2v)
        dist_matrix = sch.distance.pdist(topic_comps, 'euclidean')
        link_matrix = sch.linkage(dist_matrix, method='average')
        cophenet, cophenet_dist = sch.cophenet(link_matrix, dist_matrix)
        self.logger.info("topic cluster cophenet is [%s]" % cophenet)
        self.num_topic_clusters = self.topic_conf['num_topics'] // self.cluster_conf['num_clusters_factor']
        self.logger.info("topic cluster number is [%d]" % self.num_topic_clusters)
        sch_t2c = None
        if self.num_topic_clusters < 2:
            self.logger.error("too small topic cluster number")
        else:        
            sch_t2c= sch.fcluster(link_matrix, t=self.num_topic_clusters, criterion='maxclust')
            with open(self.t2c_file, "w") as fo:
                fo.write("\n".join(map(str, sch_t2c)))
            if paint:
                self._paint(full_t2v, sch_t2c, link_matrix, is_topic=True)
        self.logger.info("end make topic cluster cost %ds" % (time.time() - start_time))
        return sch_t2c

    def _get_top_cluster_docs(self, d2c):
        pos_docs, d2c_idx, tfidf_idx = None, None, None
        start_time = time.time()
        self.logger.info("start get top cluster docs...")
        if d2c is None:
            self.logger.error("doc cluster is not valid")
        else:
            top_cluster_num = int(self.cluster_conf['d2c_top_ratio'] * self.num_doc_clusters)
            self.logger.info("top cluster number is [%d]" % top_cluster_num)
            if top_cluster_num < 1:
                self.logger.error("too small top cluster ratio")
            else:
                cluster_counts = np.bincount(d2c)
                top_clusters = np.argsort(cluster_counts)[-top_cluster_num:]
                self.logger.info("top cluster is [%s]" % ", ".join(map(str, top_clusters)))
                self.logger.info("top cluster doc num is [%s]" % ", ".join(map(str, cluster_counts[top_clusters])))
                for (minc, maxc) in [(0, 1), (1, 20), (21, 40), (41, 60), (61, 80), (81, 100), (100, None)]:
                    self.logger.info("cluster_doc_num:(%s-%s]\t\tcluster_num:%s" % (minc, maxc if maxc is not None else 'inf', len(np.where((minc<cluster_counts)&(cluster_counts<=maxc if maxc is not None else sys.maxsize))[0])))
                d2c_idx = np.where(d2c == top_clusters[:, None])[1]
                top_cluster_dids = np.array(self.sentiment_ids)[d2c_idx]                
                np_corpus_pos_docs = np.array(self.corpus.corpus_pos_docs)
                tfidf_idx = np.where(np_corpus_pos_docs[:, 0] == top_cluster_dids[:, None])
                tfidf_idx = tfidf_idx[1] if tfidf_idx is not None and len(tfidf_idx) == 2 else None
                if tfidf_idx is None:
                    self.logger.error("top cluster doc pos is not valid")
                else:
                    pos_docs = np_corpus_pos_docs[tfidf_idx]
                    self.logger.info("top cluster doc total num is [%d]" % len(pos_docs))
                    for did, title in pos_docs[:, :2][-10:]:
                        self.logger.info("top cluster doc sample: %s-%s" % (did, title))
            self.logger.info("end get top cluster docs")
        return pos_docs, d2c_idx, tfidf_idx

    def _keywords_by_doc_textrank(self, pos_docs=None):
        #from gensim.summarization import keywords
        #for pos_content in pos_docs[:, 2]:
        #    words = keywords(content, ratio=0.03)
        #    self.logger.info(", ".join(words))
        start_time = time.time()
        self.logger.info("start get keywords by textrank%s..." % (" using top clusters" if pos_docs is not None else "")) 
        pos_docs_content = np.array(self.corpus.corpus_pos_docs)[:, 2] if pos_docs is None else pos_docs[:, 2]
        keywords = {}
        for pos_content in pos_docs_content:
            keyword_scores = self.corpus.rank_keyword(pos_content, win_size=self.textrank_conf['win_size'], max_num=self.textrank_conf['max_num'], filter_words=self.filter_keywords)
            for word, score in keyword_scores:
                if word in keywords:
                    keywords[word] += 1
                else:
                    keywords[word] = 1
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        keywords = sorted_keywords[:self.keyword_conf['num_keywords']]
        self.logger.info("keywords by textrank: %s" % keywords)
        self.logger.info("end keywords by textrank%s" % (" using top clusters" if pos_docs is not None else "")) 
        return keywords

    def _keywords_by_doc_tfidf(self, tfidf_idx=None):
        start_time = time.time()
        self.logger.info("start get keywords by tfidf%s..." % (" using top clusters" if tfidf_idx is not None else "")) 
        top_cluster_doc_tfidf = self.corpus.tfidf[tfidf_idx] if tfidf_idx is not None else self.corpus.tfidf
        keywords = {}
        for doc_tfidf in top_cluster_doc_tfidf:
            for word, score in doc_tfidf:
                if word in keywords:
                    keywords[word] += score
                else:
                    keywords[word] = score
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        filter_wids = [self.corpus.dictionary.token2id.get(word, -1) for word in self.filter_keywords]
        sorted_keywords = list(filter(lambda x: x[0] not in filter_wids, sorted_keywords))
        keywords = sorted_keywords[:self.keyword_conf['num_keywords']]
        keywords = [(self.corpus.dictionary[kw[0]], kw[1]) for kw in keywords]
        self.logger.info("keywords by tfidf: %s" % keywords)
        self.logger.info("end keywords by tfidf%s" % (" using top clusters" if tfidf_idx is not None else "")) 
        return keywords

    def _keywords_by_doc_topic(self, d2c_idx=None):
        start_time = time.time()
        self.logger.info("start get keywords by topic%s..." % (" using top clusters" if d2c_idx is not None else ""))
        lda_topics = self.lda_model.show_topics(num_topics=self.topic_conf['num_topics'], num_words=self.topic_conf['num_words'], formatted=False)
        topic_score = np.array([1/self.topic_conf['num_topics']] * self.topic_conf['num_topics'])
        if d2c_idx is not None:
            topic_score = self._top_cluster_topic_centroid(d2c_idx)
        keywords = {}
        for tid, topic in lda_topics:
            tpc_score = topic_score[tid]
            for word, score in topic:
                if word in keywords:
                    keywords[word] = (keywords[word][0]+(score * tpc_score), keywords[word][1]+1)
                else:
                    keywords[word] = ((score * tpc_score), 1)
        keywords = [(it[0], it[1][0]) for it in keywords.items()]
        sorted_keywords = sorted(keywords, key=lambda x: x[1], reverse=True)
        sorted_keywords = list(filter(lambda x: x[0] not in self.filter_keywords, sorted_keywords))
        keywords = sorted_keywords[:self.keyword_conf['num_keywords']]
        self.logger.info("keywords by topic: %s" % keywords)
        self.logger.info("end keywords by topic%s" % (" using top clusters" if d2c_idx is not None else "")) 
        return keywords

    def _top_cluster_topic_centroid(self, d2c_idx):
        lda_doc_topics = self.lda_d2v[d2c_idx]
        topic_score = [0] * self.topic_conf['num_topics']
        for doc_topics in lda_doc_topics:
            for tid, score in doc_topics:
                topic_score[tid] += score
        topic_centroid = np.array(topic_score) / len(lda_doc_topics)      
        #self.logger.info(topic_centroid[np.argsort(topic_centroid)[-10:]])
        return topic_centroid

    def _keywords_by_topic_word(self):
        pass

    def _get_topic_pattern(self, keywords):
        texts = [d[2].split(' ') for d in self.corpus.corpus_pos_docs]
        texts = [" ".join([wp.split('/')[0] for wp in wps]) for wps in texts]
        vocabs = []
        for kw in keywords:
            vocabs.append("%s 主题"%kw[0])
            vocabs.append("%s 概念"%kw[0])
            vocabs.append("%s 概念股"%kw[0])
            vocabs.append("%s 题材"%kw[0])
            vocabs.append("%s 板块"%kw[0])
        vectorizer = CountVectorizer(min_df=1, ngram_range=(2,2), vocabulary=vocabs)
        bigram_tf = vectorizer.fit_transform(texts)
        bigram_voc = vectorizer.get_feature_names()
        bigram_count = []
        for i in range(bigram_tf.shape[1]):
            bigram_count.append(bigram_tf[:, i].sum())
        bigram_tc = zip(bigram_voc, bigram_count)
        bigram_tc = list(filter(lambda x: x[1]>0, bigram_tc))
        self.logger.info("bigram term count is %s" % bigram_tc)
        return bigram_tc

    def _get_neighbor_stock(self, bigrams):
        texts = [d[2].split(' ') for d in self.corpus.corpus_pos_docs]
        texts = [" ".join([wp.split('/')[0] for wp in wps]) for wps in texts]
        stock_tc = {}
        for bg in bigrams:
            tpc = bg[0]
            for doc in texts:
                idx1 = doc.find(tpc)
                while idx1 != -1:
                    idx2 = idx1+len(tpc)
                    stop1 = doc.rfind("。", 0, idx1)
                    substr = doc[stop1+1:idx1] if stop1 != -1 else ""
                    words = substr.split(' ')
                    sns = [word for word in words if self.tushare.is_stock_name(word)]
                    stop2 = doc.rfind("。", 0, stop1) if stop1 != -1 else -1
                    substr = doc[stop2+1:stop1] if stop2 != -1 else ""
                    words = substr.split(' ')
                    sns.extend([word for word in words if self.tushare.is_stock_name(word)])
                    stop1 = doc.find("。", idx2)
                    substr = doc[idx2:stop1] if stop1 != -1 else ""
                    words = substr.split(' ')
                    sns.extend([word for word in words if self.tushare.is_stock_name(word)])
                    stop2 = doc.find("。", stop1+1) if stop1 != -1 else -1
                    substr = doc[stop1+1:stop2] if stop2 != -1 else ""
                    words = substr.split(' ')
                    sns.extend([word for word in words if self.tushare.is_stock_name(word)])                    
                    for sn in sns:
                        stock_tc[sn] = stock_tc.get(sn, 0) + 1
                    doc = doc[stop2 if stop2 != -1 else stop1 if stop1 != -1 else idx2:]
                    idx1 = doc.find(tpc)
        self.logger.info(stock_tc)
        return stock_tc

    def _paint(self, d2v, d2c, link_matrix, is_3d=False, is_topic=False):
        start_time = time.time()
        self.logger.info("start paint...")
        plt.figure(figsize=(15, 8))
        if is_3d:
            from mpl_toolkits.mplot3d import Axes3D
            tsne_plt = plt.subplot(311, projection='3d')
        else:
            tsne_plt = plt.subplot(311)
        model = TSNE(n_components=3 if is_3d else 2, init='pca', random_state=0)
        reduced_vecs = model.fit_transform(d2v)
        colors =  itertools.cycle(['y', 'b', 'g', 'r', 'c', 'k', 'm'])
        for i in range(self.num_topic_clusters if is_topic else self.num_doc_clusters):
            argidx = np.where(d2c==i+1)
            if is_3d:
                tsne_plt.scatter(reduced_vecs[argidx, 0], reduced_vecs[argidx, 1], reduced_vecs[argidx, 2], color=next(colors), marker='.', label="c%s"%i, s=20)#, markersize=8)
            else:
                tsne_plt.scatter(reduced_vecs[argidx, 0], reduced_vecs[argidx, 1], color=next(colors), marker='.', label="c%s"%i, s=20)#, markersize=8)
        #tsne_plt.legend(loc='upper left', ncol=10, fontsize=8, scatterpoints=1)#, numpoints=1, bbox_to_anchor=(0, 0)) 
        cluster_count_plt = plt.subplot(312)
        cluster_count_plt.set_ylabel('topic_num' if is_topic else 'doc_num')
        cluster_count_plt.set_xlabel('cluster_id')
        cluster_counts = np.bincount(d2c)
        cluster_count_plt.plot(range((self.num_topic_clusters if is_topic else self.num_doc_clusters)+1), cluster_counts, 'r')
        tree_plt = plt.subplot(313)
        tree_dict = sch.dendrogram(link_matrix, truncate_mode='lastp', p=self.num_topic_clusters if is_topic else self.num_doc_clusters, show_leaf_counts=True, leaf_rotation=90., leaf_font_size=12., show_contracted=False)
        #plt.show()
        plt.savefig(self.t2c_fig_file if is_topic else self.d2c_fig_file)
        self.logger.info("end paint cost %ds" % (time.time() - start_time))
