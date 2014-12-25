# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import codecs

from extractor.extractor import *
import util

global results

#from extractor.source_mysql import DataSourceMySQL as DataSource
from extractor.source_fjson import DataSourceFJson as DataSource

folder = 'G:/EXP-07/'
dSource = DataSource(folder)

def extract(uid):
    try:
        profile  = dSource.get_profile(uid)
        v = [uid]
        if profile is not None:
            u = extract_profile(profile)

            statuses = dSource.get_statuses(uid)
            v1 = extract_statuses_behave(statuses)
            v2 = extract_statuses_text(statuses)
            v += u+v1+v2
        return v
    except RuntimeError as e:
        return None

if __name__ == '__main__':
    from multiprocessing import Pool, freeze_support
    pool = Pool(3)
    freeze_support()

    output_file = folder + '/BasicFeatures.csv'
    uid_file = folder + "/UserList.txt"
    uids = util.readlines(uid_file)#[:100]

    #results = [extract(uid) for uid in uids]
    results = pool.map(extract,uids)

    with codecs.open(output_file, 'w+', encoding='utf-8') as fp:
        fp.write(u'\uFEFF')
        for result in results:
            if result is None:
                continue
            line = ','.join(result)
            fp.write(line + '\n')
