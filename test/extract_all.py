# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import codecs
import util
from extractor.source_mysql import DataSourceMySQL as DataSource
from extractor.extractor import *

dSource = DataSource()

def extract(uid):
    profile  = dSource.get_profile(uid)
    if profile is None:
        return []
    statuses = dSource.get_statuses(uid)

    u = extract_profile(profile)
    v1 = extract_statuses_behave(statuses)
    v2 = extract_statuses_text(statuses)
    return u+v1+v2


if __name__ == '__main__':
    output_file = 'G:/EXP-Data/BasicFeatures.csv'
    uid_file = r"E:\Study\Publishing\TimeSequencePaper\data\UserList_hasSeq.txt"
    uid_list = util.readlines(uid_file)

    with codecs.open(output_file, 'w', encoding='utf-8') as fp:
        fp.write(u'\uFEFF')
        for uid in uid_list:
            v = extract(uid)
            line = ','.join([uid]+v)
            print(uid)
            fp.write(line + '\n')