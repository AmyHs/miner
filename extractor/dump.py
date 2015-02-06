#  -*- encoding=utf-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import os
import json
import util

# from weibo.source_mysql import DataSourceMySQL as Source
from weibo.source_hbase import DataSourceHBase as Source


'''
cfg = {}
cfg.update(source_mysql._cfg)
cfg['db'] = 'weibo_bj1w'
source = Source(cfg=cfg)
'''

folder = "D:/Dump/"

for dir in ['Profile', 'Status']:
    try:
        os.mkdir(folder + dir)
    except:
        pass


def dump_profile(uid, path=folder + '/Profile/'):
    profile = Source().get_profile(uid)
    if profile is None:
        return
    with open(path+uid+'.profile', 'w') as fp:
        if not isinstance(profile, dict):
            profile = profile.__dict__
        json.dump(profile, fp, encoding='utf-8', indent=1, sort_keys=True, ensure_ascii=False, default=util.time2str)


def dump_status(uid, path=folder + '/Status/'):
    statuses = Source().get_statuses(uid)
    if statuses is None:
        return
    statuses = [s if isinstance(s, dict) else s.__dict__ for s in statuses]
    if len(statuses) == 0:
        return
    else:
        for status in statuses:
            for attr in ['seg', 'seg_c', 'ltp', 't_cl']:
                status.pop(attr, None)
    with open(path+uid+'.json', 'w') as fp:
        json.dump(statuses, fp, encoding='utf-8', indent=1, ensure_ascii=False, default=util.time2str)


def dump_user_data(uid):
    print(uid)
    try:
        dump_profile(uid)
        dump_status(uid)
    except IOError as e:
        print e
        pass


if __name__ == '__main__':
    from multiprocessing import Pool, freeze_support

    uids = util.readlines(folder + "UserList.txt")[5000]
    # [dump_user_data(uid) for uid in uids]

    freeze_support()
    pool = Pool()
    pool.map(dump_user_data, uids)
    pool.close()