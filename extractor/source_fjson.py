# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import os,json,codecs

def get_fjson(fjson_path):
    if not os.path.exists(fjson_path):
        raise RuntimeError('Could not open given JSON file [%s].' % fjson_path)
    with codecs.open(fjson_path,'r',encoding='utf-8') as fp:
        return json.load(fp,encoding='utf-8')

class DataSourceFJson:
    def __init__(self,base_dir):
        self.dir_profile = base_dir + r"Profile/"
        self.dir_status  = base_dir + r"Status/"

    def get_profile(self,uid):
        fname = "%s%s.profile" % (self.dir_profile,uid)
        return get_fjson(fname)

    def get_statuses(self,uid,do_yield=False):
        fname = "%s%s.json" % (self.dir_status, uid)
        statuses = get_fjson(fname)
        return statuses