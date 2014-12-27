# -*- coding: utf-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

'''
此脚本用于根据csv格式文件的第一列作为主键，将多个文件依主键按行合并
文件中，列分隔符可以是Tab键，或者半角英文逗号
'''

import sys,codecs
from collections import OrderedDict


def mergeCol(files, outputfile):
    header = ["SinaUid"]
    data = OrderedDict()

    for flag,fname in files.iteritems():
        with codecs.open(fname,'r',encoding='utf-8-sig') as fp:
            line = fp.readline()

            spliter = "," if "," in line else "\t"

            #表头处理
            headerCols = line.strip(' \t\r\n,').split(spliter)[1:]
            for col in headerCols:
                header.append(flag + col)

            #数据从第二行开始
            for line in fp:
                line = line.strip(' \t\r\n,')
                fields = line.split(spliter)

                #第一列是UID，第二列开始是特征数据
                uid = fields[0].rstrip(".txt")
                values = fields[1:]

                if uid not in data:
                    data[uid] = list()

                for value in values:
                    data.get(uid).append(value)

    nCol = len(header) - 1
    f = codecs.open(outputfile, "w", encoding='utf-8')

    f.write(u'\uFEFF')
    f.write(header[0])
    for h in header[1:]:
        f.write(","+h)

    for uid,fields in data.iteritems():
        if len(fields) < nCol: continue
        f.write("\n" + uid + ',')
        f.write(",".join(data.get(uid)))

'''
Month = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
for month in Month:
    files=OrderedDict([
        ('',  u"E:/features/byMonth/%s.csv" % month),
        ('_',  u"E:/features/f_B.csv"),
    ])
    outputFile = r"E:/features/byMonthMerged/[%s].csv" % month
    mergeCol(files,outputFile)
'''

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print 'Usage: python merge_join.py PREFIX_file1 PREFIX_file2 [PREFIX_fileN...] result_file'
        exit(-1)

    outputFile = sys.argv[-1]

    files = OrderedDict()
    for arg in sys.argv[1:-1]:
        print arg
        args = arg.split('_')
        if len(args)>1:
            prefix, fname = args
        else:
            prefix, fname = '', args[0]
        files[prefix+'_'] = fname

    mergeCol(files, outputFile)