# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import util,MySQLdb,codecs
from collections import defaultdict
from datetime import datetime
from textmind.wenxin import TextMind

cfg = {
    'host':     "192.168.21.74",
    'user':     "root",
    'passwd':   "wsi_208",
    "charset":  "utf8",

    "db":       "weibo_2000"
}

con = MySQLdb.connect(**cfg)
cur = con.cursor (MySQLdb.cursors.DictCursor)

def get_user_list_from_file(fpath):
    result = []
    with codecs.open(fpath,'r',encoding='utf-8-sig') as fp:
        for line in fp:
            line = line.strip(' \t\r\n')
            result.append(line)
    return result


def get_user_list():
    cur.execute('SELECT * FROM sinauids')
    result = []
    for uid in cur:
        result.append(uid.get('SinaUid'))
    return result

def get_status(uid):
    cur.execute('select created_at, text from sina_statuses where user_id=%s' % uid)
    result = defaultdict(str)
    for s in cur:
        time = s.get('created_at')
        time = util.time2epoch(time)
        time = datetime.fromtimestamp(time)

        (year, week, _) = time.isocalendar()
        key = "%04d%02d" % (year,week)

        text = s.get('text')
        tval = result.get(key,None)
        if tval is not None:
            result[key] = tval + '\n' + text
        else:
            result[key] = text
    return result


weeks = ['2011%s' % i for i in range(1,52)].extend(['2012%s' % i for i in range(1,52)])

base_dir = 'G:/EXP-Data/'#'J:/Yu-Data/'

if __name__ == '__main__':
    #uid_list = get_user_list()[:600]
    uid_list = get_user_list_from_file(r"E:\Study\Publishing\AAAI2015\data\UserList.txt")[5]

    result = {}

    for uid in uid_list:
        statuses = get_status(uid)

        keys = sorted(statuses.iterkeys())
        for k in keys:
            textMind = TextMind()
            t = statuses.get(k)
            v = t.encode('utf-8')

            if len(v)>0:
                k2 = '%s#%s' % (uid, k)
                try:
                    vec = textMind.process_paragraph(v,encoding='utf-8')
                    result[k2] = vec
                except WindowsError as e:
                    pass

    cols = vec._results.iterkeys()
    for col in cols:
        fpath = base_dir + (col.replace('/','_'))
        with open(fpath, 'w') as fp:
            for week in weeks:
                k = '%s\t' % week
                fp.write(k)

            for uid in uid_list:
                fp.write('\n%s\t' % uid)

                for week in weeks:
                    k = '%s#%s' % (uid, week)
                    vector = result.get(k,None)
                    if vector is None:
                        val = 0
                    else:
                        val = vector.__getitem__(col)
                    fp.write('%s\t' % val)
