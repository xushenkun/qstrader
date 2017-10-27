#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, string
import six, inspect
from importlib import import_module
from pkgutil import iter_modules

def walk_modules(path):
    mods = []
    mod = import_module(path)
    mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                mods.append(submod)
    return mods    

def load_classes(module_name, cls):
    classes = {}
    mods = walk_modules(module_name)
    for mod in mods:
        for obj in six.itervalues(vars(mod)):
            if inspect.isclass(obj) and obj.__module__ == mod.__name__ and getattr(obj, 'name', None):#and issubclass(obj, AbstractSentiment):# 
                classes[obj.name] = obj
    return classes    

def get_stop_word(file_path):
    stopwords = []
    with open(file_path, mode='r', encoding='utf-8') as fi:
        line = fi.readline()
        while line:
            stopwords.append(line.strip())
            line = fi.readline()  
    return stopwords  

def is_number(s):
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
    
def is_punctuation(s):
    return s in string.punctuation+'！“”（），。：；？‘’'