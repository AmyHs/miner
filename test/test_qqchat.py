# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import sqlite3 as db
import codecs
from collections import OrderedDict
from textmind.wenxin import process_iterator
from multiprocessing import Pool

db_path = "F:/Aeon/Desktop/Qingdao/chat.db"
output_file = "F:/Aeon/Desktop/Qingdao/features.csv"

def q(sql):
    print(sql)
    with db.connect(db_path) as con:
        try:
            c = con.cursor()
            c.execute(sql)
            return [i for i in c.fetchall()]
        except Exception as e:
            raise e


def stat_user():
    return q("SELECT qid, COUNT(1) messages, COUNT(DISTINCT gid) groups FROM chat GROUP BY qid ORDER BY messages DESC,groups DESC")

def stat_group():
    return q("SELECT gid, COUNT(1) messages, COUNT(DISTINCT qid) users FROM chat GROUP BY gid ORDER BY messages DESC,users DESC")

def get_user_msg(qid):
    msgs = q("SELECT msg FROM chat WHERE qid=%s ORDER BY time ASC" % qid)
    return [i[0].strip() for i in msgs]

def extract_user(qid):
    def iterate(qid):
        msgs = get_user_msg(qid)
        for msg in msgs:
            yield msg.encode('utf-8')

    _iter = iterate(qid)
    return (qid, process_iterator(_iter, enable_pos=False) )


def extract_users():
    users = OrderedDict()
    for (qid,messages,groups) in stat_user():
        users[qid] = (messages, groups)

    pool = Pool() #processes=8,
    results = pool.map(extract_user, users)
    pool.close()

    with codecs.open(output_file,'w',encoding='utf-8') as fp:
        fp.write(u'\uFEFF')
        for qid,r in results:
            u = users[qid]
            fp.write('%s,%s,%s,' % (qid, u[0], u[1]))
            r.dump(fp=fp, separator=',')
            fp.write('\n')


def stat_day():
    return q("SELECT date(time) day, count(1) msgs, count(DISTINCT qid) users, count(DISTINCT gid) groups FROM chat GROUP BY day ORDER BY day")

def get_msg_by_day(day):
    msgs = q("SELECT msg FROM chat WHERE DATE(time) = '%s' ORDER BY gid ASC, time ASC" % day)
    return [i[0].strip() for i in msgs]

def extract_day(day):
    def iterate(d):
        msgs = get_msg_by_day(d)
        for msg in msgs:
            yield msg.encode('utf-8')

    _iter = iterate(day)
    return (day, process_iterator(_iter, enable_pos=False) )

def extract_days():
    days = OrderedDict()
    for (day, msgs, users, groups) in stat_day():
        days[day] = (msgs, users, groups)

    pool = Pool() #processes=8,
    results = pool.map(extract_day, days)
    pool.close()

    print type(results)

    with codecs.open(output_file,'w',encoding='utf-8') as fp:
        fp.write(u'\uFEFF')
        for day,r in results:
            d = days[day]
            fp.write('%s,%s,%s,%s,' % (day, d[0], d[1], d[2]))
            r.dump(fp=fp, separator=',')
            fp.write('\n')


if __name__ == '__main__':
    extract_days()