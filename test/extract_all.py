# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import codecs
from multiprocessing import Pool, freeze_support

from extractor.source_mysql import DataSourceMySQL as DataSource
from extractor.extractor import *
import util

dSource = DataSource()

global results

def extract(uid):
    profile  = dSource.get_profile(uid)
    v = [uid]
    if profile is not None:
        statuses = dSource.get_statuses(uid)
        u = extract_profile(profile)
        v1 = extract_statuses_behave(statuses)
        v2 = extract_statuses_text(statuses)
        v += u+v1+v2

    return v


if __name__ == '__main__':
    freeze_support()
    output_file = 'G:/EXP-Data/BasicFeatures.csv'
    uid_file = r"E:\Study\Publishing\TimeSequencePaper\data\UserList_hasSeq.txt"
    uids = util.readlines(uid_file)#[:2]

    pool = Pool()
    results = pool.map(extract,uids)

    with codecs.open(output_file, 'w+', encoding='utf-8') as fp:
        fp.write(u'\uFEFF')
        for result in results:
            line = ','.join(result)
            fp.write(line + '\n')
