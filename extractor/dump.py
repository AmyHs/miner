#-*- encoding=utf-8 -*-
from weibo import source_mysql

__author__ = 'Peter_Howe<haobibo@gmail.com>'

import codecs,json

from weibo.source_mysql import DataSourceMySQL as Source

import util

cfg = {}
cfg.update(source_mysql._cfg)


cfg['db'] = 'weibo_bj1w'
source = Source(cfg=cfg)

folder = 'G:/Exp-06/'

def dumpProfile(uid, path = folder + '/Profile/'):
    profile = source.get_profile(uid)
    if profile is None: return
    with codecs.open(path+uid+'.profile', 'w', 'utf-8') as fp:
        json.dump(profile, fp, encoding='utf-8',indent=1, ensure_ascii=False, default=util.time2str)


def dumpStatus(uid, path = folder + '/Status/'):
    statuses = source.get_statuses(uid)
    if statuses is None or len(statuses)==0: return
    with codecs.open(path+uid+'.json', 'w', 'utf-8') as fp:
        json.dump(statuses, fp, encoding='utf-8',indent=1, ensure_ascii=False, default=util.time2str)


if __name__ == '__main__':
    from multiprocessing import Pool, freeze_support

    uids = util.readlines(folder + "UserList.txt")

    freeze_support()
    pool = Pool()
    pool.map(dumpProfile, uids)
    pool.map(dumpStatus, uids)
