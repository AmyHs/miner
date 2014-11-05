# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import MySQLdb

_cfg = {
    'host':     "192.168.21.74",
    'user':     "root",
    'passwd':   "wsi_208",
    "charset":  "utf8",

    "db":       "weibo_2000"
}

def fields2str(fields):
    if fields is None:
        _fields = '*'
    elif isinstance(fields,list):
        _fields = ','.join(fields)  #['created_at', 'text']
    else:
        _fields = fields
    return _fields

class DataSourceMySQL:
    def __init__(self,cfg=None):
        if cfg is None:
            self.cfg = _cfg
        self.con = MySQLdb.connect(**self.cfg)

    def get_profile(self,uid,fields=None):
        _fields = fields2str(fields)
        sql = 'SELECT %s FROM sina_user WHERE id=%s' % (_fields,uid)
        cur = self.con.cursor (MySQLdb.cursors.DictCursor)
        cur.execute(sql)

        u = None
        for s in cur:
            u = s
        return u


    def get_statuses(self,uid,fields=None):
        _fields = fields2str(fields)
        sql = 'SELECT %s FROM sina_statuses WHERE user_id=%s' % (_fields,uid)
        cur = self.con.cursor (MySQLdb.cursors.DictCursor)
        cur.execute(sql)
        #for s in cur:yield s
        return [s for s in cur]
