#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, struct, yaml
import logging.config

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + os.path.sep + '..')

class Sogou(object):

    def __init__(self):
        self.py_start_pos = 0x1540
        self.zh_start_pos = 0x2628
        self.py_info = {}
        self.word_info = []

    def byte2str(self, data): 
        ret = ''
        for i in range(0, len(data), 2):
            x = data[i:i+2]
            t = chr(struct.unpack('H', x)[0])
            if t == '\r':
                ret += '\n'  
            elif t != ' ':
                ret += t
        return ret

    def get_py_info(self, data):
        if data[0:4] == b"\x9d\x01\x00\x00":   #"\x9D\x01\x00\x00":
            data, pos = data[4:], 0
            length = len(data)
            while pos < length:  
                index = struct.unpack('H', data[pos:pos+2])[0]
                pos += 2
                py_len = struct.unpack('H', data[pos:pos+2])[0] 
                pos += 2
                py = self.byte2str(data[pos:pos+py_len])
                self.py_info[index] = py
                pos += py_len

    def get_word_py(self, data):  
        length, pos, ret = len(data), 0, ''
        while pos < length:              
            index = struct.unpack('H', data[pos:pos+2])[0]
            ret += self.py_info[index]
            pos += 2
        return ret

    def get_word_info(self, data):  
        pos, length = 0, len(data)
        while pos < length:
            same = struct.unpack('H', data[pos:pos+2])[0]
            pos += 2
            py_pos_len = struct.unpack('H', data[pos:pos+2])[0] 
            pos += 2  
            py = self.get_word_py(data[pos:pos+py_pos_len])
            pos += py_pos_len
            for i in range(same):
                c_len = struct.unpack('H', data[pos:pos+2])[0]
                pos += 2    
                word = self.byte2str(data[pos:pos+c_len])
                pos += c_len          
                ext_len = struct.unpack('H', data[pos:pos+2])[0]
                pos += 2  
                count  = struct.unpack('H', data[pos:pos+2])[0]  
                self.word_info.append((count,py,word))
                pos += ext_len

    def save(self, file_name):
        with open(file_name,'w',encoding='utf-8') as fo:
            for count,py,word in self.word_info:
                fo.write("%s %s\n" % (word, count))

    def process(self, file_name):
        with open(file_name,'rb') as fi:
            data = fi.read()
            if data[0:12] != b"@\x15\x00\x00DCS\x01\x01\x00\x00\x00": #\x40\x15\x00\x00\x44\x43\x53\x01\x01\x00\x00\x00":
                print("please make sure you are using sogou .scel dict?")
            else:
                print("dict name:", self.byte2str(data[0x130:0x338]).replace('\r','').replace('\n','').replace('\x00','').strip())
                print("dict type:", self.byte2str(data[0x338:0x540]).replace('\r','').replace('\n','').replace('\x00','').strip())
                print("dict desc:", self.byte2str(data[0x540:0xd40]).replace('\r','').replace('\n','').replace('\x00','').strip())
                print("dict samp:", self.byte2str(data[0xd40:self.py_start_pos]).replace('\r','').replace('\n',' ').replace('\x00','').strip())
                self.get_py_info(data[self.py_start_pos:self.zh_start_pos]) 
                self.get_word_info(data[self.zh_start_pos:])
        self.save(file_name+'.txt')

def merge_helper():
    files=["userdict_sogou.txt", "userdict_tsinghua.txt"]
    word_freq_dict = {}
    for file in files:
        print("merge", file)
        with open(file, 'r', encoding='utf-8') as fi:
            line = fi.readline()
            while line:
                line = line.strip()
                if line:
                    try:
                        word_freq = line.split(' ')
                        if len(word_freq) != 2:
                            print(line)
                        else:
                            if int(word_freq[1]) > word_freq_dict.get(word_freq[0], 0):
                                word_freq_dict[word_freq[0]] = int(word_freq[1])
                    except Exception as e:
                        print(line)
                line = fi.readline()
    with open('userdict.txt', 'w', encoding='utf-8') as fo:
        items = word_freq_dict.items()
        for word_freq in items:
            fo.write("%s %s\n" % (word_freq[0], word_freq[1]))


if __name__ == '__main__':
    assert len(sys.argv) == 2, "Sogou script should have enought arguments like: python sogou.py scel_file"
    scel_file = sys.argv[1]
    #sogou = Sogou()
    #sogou.process(scel_file)  
    merge_helper()      