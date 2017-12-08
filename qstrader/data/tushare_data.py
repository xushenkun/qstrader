#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import logging.config

import pandas as pd
import tushare as ts

class TushareData(object):

    name = "Tushare Data"

    def __init__(self, global_conf, full, conf, logger=None):
        self.global_conf = global_conf
        self.data_root_path = self.global_conf['data_path']
        self.full = full
        self.config(conf)
        self.logger = logger if logger is not None else logging.getLogger('tushare')
        self._stock_codes = None
        self._stock_names = None
        self.init_stock()

    def config(self, conf):
        now = datetime.datetime.now()
        today = now.strftime("%Y-%m-%d")
        self.basic_stock_file = os.path.join(self.data_root_path, conf['basic_stock_file']) % today        

    def init_stock(self):
        if self._stock_codes is None:
            if not os.path.exists(self.basic_stock_file):
                stocks = ts.get_stock_basics()
                stocks.to_csv(self.basic_stock_file, encoding='utf-8')
            else:
                stocks = pd.read_csv(self.basic_stock_file, encoding='utf-8')
            self._stock_codes = stocks.values[:, 0].tolist()
            self._stock_names = stocks.values[:, 1].tolist()

    def get_stock_names(self):
        return self._stock_names

    def is_stock_code(self, s):
        return s in self._stock_codes

    def is_stock_name(self, s):
        return s in self._stock_names