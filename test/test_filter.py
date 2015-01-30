# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

from util import *

base_dir = r"J:/"
data_file = base_dir + u'第七次用户实验（自杀可能性）.csv'
ignore =    base_dir + u'normal-users.txt'
out_put =   base_dir + u'output.csv'

cleaned = readlines(ignore)
data    = readlines(data_file)


with codecs.open(out_put,'w','utf-8') as fp:
    fp.write(u'\uFEFF')
    fp.write(data[0])
    fp.write('\n')

    for datum in data[1:]:
        row = datum.split(',')
        rowkey = row[0]
        cols = row[1:]
        if rowkey in cleaned:
            fp.write(rowkey)
            fp.write(',')
            fp.write(','.join(cols))
            fp.write('\n')


