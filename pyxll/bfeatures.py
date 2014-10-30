# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

from dateutil import parser
from pyxll import xl_func

from extractor.source_fjson import DataSourceFJson
from extractor.extractor import extract_statuses_text,extract_statuses_behave

base_dir = r"J:/UserData/"
dsource = DataSourceFJson(base_dir)


@xl_func("str uid, str attr: int",thread_safe=True, volatile=False)
def get_uattr(uid,attr):
    """returns specific attr for user with given uid"""
    try:
        t = dsource.get_profile(uid)
        attrs = attr.split('/')
        for a in attrs:
            if t is not None and isinstance(t,dict):
                t = t.get(a,'N/A[%s]-U%s' % (a,uid))
        return t
    except Exception as e:
        return e.message


@xl_func("str uid, str attr: string",thread_safe=True, volatile=False)
def get_uattr_str(uid,attr):
    x = get_uattr(uid,attr)
    if isinstance(x,unicode):
        x = x.encode('gbk','ignore')
    return x


@xl_func("str uid, string date_filter: string[]",category="CCPL-Category",thread_safe=True, volatile=False)
def stat_statuses_text(uid, date_filter=None):
    try:
        statuses = dsource.get_statuses(uid)
        if date_filter is None: date_filter = 'Dec 12 23:59:59 +0800 2099'
        filter = parser.parse(date_filter,fuzzy=True)
        return [extract_statuses_text(statuses, filter)]
    except Exception as e:
        return [['N/A' for i in range(102)]]


@xl_func("str uid, string date_filter: string[]",category="CCPL-Category", thread_safe=True, volatile=False)
def stat_statuses(uid, date_filter=None):
    try:
        statuses = dsource.get_statuses(uid)
        if date_filter is None: date_filter = 'Dec 12 23:59:59 +0800 2099'
        filter = parser.parse(date_filter,fuzzy=True)
        return [extract_statuses_behave(statuses, filter)]
    except Exception as e:
        return [['N/A' for i in range(23)]]
