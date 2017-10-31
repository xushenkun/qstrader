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
from gensim import corpora, models, matutils
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
        self.topic_conf = conf['topic']
        self.cluster_conf = conf['cluster']
        self.keyword_conf = conf['keyword']
        self.d2c_fig_file = os.path.join(self.out_path, self.cluster_conf['d2c_fig_file'])
        self.d2c_file = os.path.join(self.out_path, self.cluster_conf['d2c_file'])

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
                    num_topics=self.topic_conf['num_topics'], eval_every=self.topic_conf['eval_every'], batch=False, chunksize=self.topic_conf['chunksize'], passes=self.topic_conf['passes'], workers=self.topic_conf['workers'])
                self.lda_model.save(self.out_tpc_file)
                self.lda_d2v = self.lda_model[self.corpus.tfidf]
                corpora.MmCorpus.serialize(self.out_d2v_file, self.lda_d2v)
            with open(self.out_id_file,'w') as fo:
                fo.write("\n".join(self.sentiment_ids))
            self._cluster()
            self._top_cluster_docs()
            #self._keywords()
            self._keywords_by_tfidf()
            self._keywords_by_lda(with_cluster=False)
            self._keywords_by_lda()
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

    def _cluster(self):
        start_time = time.time()
        self.logger.info("start cluster...")        
        full_d2v = np.empty(shape=(len(self.lda_d2v), self.topic_conf['num_topics']))
        for i, v in enumerate(self.lda_d2v):
            v = matutils.unitvec(matutils.sparse2full(v, self.topic_conf['num_topics']))
            full_d2v[i] = v
        dist_matrix = sch.distance.pdist(full_d2v, 'euclidean')
        link_matrix = sch.linkage(dist_matrix, method='average')
        cophenet, cophenet_dist = sch.cophenet(link_matrix, dist_matrix)
        self.logger.info("cluster cophenet is %s" % cophenet)
        self.sch_d2c= sch.fcluster(link_matrix, t=self.cluster_conf['num_clusters'], criterion='maxclust')
        with open(self.d2c_file, "w") as fo:
            fo.write("\n".join(map(str, self.sch_d2c)))
        #self._paint(full_d2v, self.sch_d2c, link_matrix)
        self.logger.info("end cluster cost %ds" % (time.time() - start_time))

    def _top_cluster_docs(self):
        start_time = time.time()
        self.logger.info("start get top cluster docs...") 
        top_cluster_num = int(self.cluster_conf['d2c_top_ratio'] * self.cluster_conf['num_clusters'])
        cluster_count = np.bincount(self.sch_d2c)
        top_clusters = np.argsort(cluster_count)[-top_cluster_num:]
        self.logger.info("top cluster is %s" % ", ".join(map(str, top_clusters)))
        self.logger.info("top cluster doc num is %s" % ", ".join(map(str, cluster_count[top_clusters])))
        for (minc, maxc) in [(0, 1), (1, 20), (21, 40), (41, 60), (61, 80), (81, 100), (100, None)]:
            self.logger.info("cluster_doc_num:(%s-%s]\t\tcluster_num:%s" % (minc, maxc if maxc is not None else 'inf', len(np.where((minc<cluster_count)&(cluster_count<=maxc if maxc is not None else sys.maxsize))[0])))
        self.top_cluster_d2c_pos = np.where(self.sch_d2c == top_clusters[:, None])[1]
        top_cluster_dids = np.array(self.sentiment_ids)[self.top_cluster_d2c_pos]
        np_corpus_docs = np.array(self.corpus.corpus_docs)
        self.top_cluster_doc_pos = np.where(np_corpus_docs[:, 0] == top_cluster_dids[:, None])[1]
        self.top_cluster_docs = np_corpus_docs[self.top_cluster_doc_pos]
        self.logger.info("top cluster doc total num is %d" % len(self.top_cluster_docs))
        for did, title in self.top_cluster_docs[:, :2][-10:]:
            self.logger.info("top cluster sample doc: %s-%s" % (did, title))
        self.logger.info("end get top cluster docs")         

    def _keywords_by_textrank(self):
        from gensim.summarization import keywords
        for content in self.top_cluster_docs[:, 2]:
            words = keywords(content, ratio=0.03)
            self.logger.info(", ".join(words))

    def _keywords_by_tfidf(self):
        start_time = time.time()
        self.logger.info("start get keywords by tfidf...") 
        top_cluster_doc_tfidf = self.corpus.tfidf[self.top_cluster_doc_pos]
        keywords = {}
        for doc_tfidf in top_cluster_doc_tfidf:
            for tfidf in doc_tfidf:
                word = tfidf[0]
                score = tfidf[1]
                if word in keywords:
                    keywords[word] += score
                else:
                    keywords[word] = score
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        keywords = sorted_keywords[:self.keyword_conf['num_keywords']]
        keywords = [(self.corpus.dictionary[kw[0]], kw[1]) for kw in keywords]
        self.logger.info("keywords by tfidf: %s" % keywords)
        self.logger.info("start end keywords by tfidf") 
        return keywords

    def _keywords_by_lda(self, with_cluster=True):
        start_time = time.time()
        self.logger.info("start get keywords by lda%s..." % (" with cluster" if with_cluster else ""))
        lda_topics = self.lda_model.show_topics(num_topics=self.topic_conf['num_topics'], num_words=self.topic_conf['num_words'], formatted=False)
        topics_score = {}
        if with_cluster:
            lda_doc_topics = self.lda_d2v[self.top_cluster_d2c_pos]
            topic_score = {}
            for doc_topics in lda_doc_topics:
                for tid, score in doc_topics:
                    if tid in topic_score:
                        topics_score[tid] += score
                    else:
                        topics_score[tid] = score
        keywords = {}
        for tid, topic in lda_topics:
            if with_cluster:
                if tid not in topics_score.keys():
                    continue
                else:
                    tpc_score = topics_score[tid]
            else:
                tpc_score = 1
            for word, score in topic:
                if word in keywords:
                    keywords[word] += (score * tpc_score)
                else:
                    keywords[word] = (score * tpc_score)
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        keywords = sorted_keywords[:self.keyword_conf['num_keywords']]
        self.logger.info("keywords by lda: %s" % keywords)
        self.logger.info("start end keywords by lda%s" % (" with cluster" if with_cluster else "")) 
        return keywords

    def _paint(self, d2v, d2c, link_matrix, is_3d=False):
        start_time = time.time()
        self.logger.info("start paint...")
        plt.figure(figsize=(15, 8))
        if is_3d:
            from mpl_toolkits.mplot3d import Axes3D
            tsne_plt = plt.subplot(211, projection='3d')
        else:
            tsne_plt = plt.subplot(211)
        model = TSNE(n_components=3 if is_3d else 2, init='pca', random_state=0)
        reduced_vecs = model.fit_transform(d2v)
        colors =  itertools.cycle(['y', 'b', 'g', 'r', 'c', 'k', 'm'])
        for i in range(self.cluster_conf['num_clusters']):
            argidx = np.where(d2c==i+1)
            if is_3d:
                tsne_plt.scatter(reduced_vecs[argidx, 0], reduced_vecs[argidx, 1], reduced_vecs[argidx, 2], color=next(colors), marker='.', label="c%s"%i, s=20)#, markersize=8)
            else:
                tsne_plt.scatter(reduced_vecs[argidx, 0], reduced_vecs[argidx, 1], color=next(colors), marker='.', label="c%s"%i, s=20)#, markersize=8)
        #tsne_plt.legend(loc='upper left', ncol=10, fontsize=8, scatterpoints=1)#, numpoints=1, bbox_to_anchor=(0, 0)) 
        tree_plt = plt.subplot(212)
        tree_dict = sch.dendrogram(link_matrix, truncate_mode='lastp', p=self.cluster_conf['num_clusters'], show_leaf_counts=True, leaf_rotation=90., leaf_font_size=12., show_contracted=False)
        #plt.show()
        plt.savefig(self.d2c_fig_file)
        self.logger.info("end paint cost %ds" % (time.time() - start_time))
