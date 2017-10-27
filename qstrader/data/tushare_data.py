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

    def config(self, conf):
        now = datetime.datetime.now()
        today = now.strftime("%Y-%m-%d")
        self.basic_stock_file = os.path.join(self.data_root_path, conf['basic_stock_file']) % today

    def get_stock_codes(self, refresh=False):
        if self._stock_codes is None or refresh:
            if not os.path.exists(self.basic_stock_file):
                stocks = ts.get_stock_basics()
                stocks.to_csv(self.basic_stock_file, encoding='utf-8')
            else:
                stocks = pd.read_csv(self.basic_stock_file, encoding='utf-8')
            self._stock_codes = stocks.values[:, 0].tolist()
        return self._stock_codes

    def is_stock_code(self, s):
        return s in self.get_stock_codes()