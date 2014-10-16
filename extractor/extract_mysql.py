# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import util,MySQLdb,codecs,json
from collections import defaultdict,OrderedDict
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


base_dir = 'G:/EXP-Data/'

def process_users(uid_list):
    for uid in uid_list:
        u_result = {}

        with codecs.open('%sOriginal/%s.json' % (base_dir, uid), 'w', encoding='utf-8') as fp:
            statuses = get_status(uid)
            keys = sorted(statuses.iterkeys())
            for k in keys:
                textMind = TextMind()
                t = statuses.get(k)
                v = t.encode('utf-8')

                if len(v)>0:
                    vec = textMind.process_paragraph(v,encoding='utf-8').dump(separator=',')
                    u_result[k] = vec

            json.dump(u_result, fp, indent=1)

def process_features(uid_list,time_list):
    header = TextMind().get_header()

    fw_pool = OrderedDict()
    for head in header:
        k = head.replace('/','_')
        v = '%sTimeSequence/%s.csv' % (base_dir, k)
        fp = open(v, 'w+')
        fw_pool[head] = fp

    for uid in uid_list:
        with codecs.open('%sOriginal/%s.json' % (base_dir, uid), 'r', encoding='utf-8-sig') as fp:
            u_seq = json.load(fp,encoding='utf-8')

            for t in time_list:
                vec = u_seq.get(t,None)
                features = [0] * len(header) if vec is None else vec.split(',')

                iter = 0
                for head,fw in fw_pool.iteritems():
                    fw.write( '%s,' % features[iter] )
                    iter += 1

            for fw in fw_pool.values():
                fw.write('\n')

if __name__ == '__main__':
    #uid_list = get_user_list()[:600]
    uid_list = get_user_list_from_file(r"E:\Study\Publishing\TimeSequencePaper\data\UserList.txt")
    time_list = ['2011%s' % i for i in range(1,52)] + ['2012%s' % i for i in range(1,36)]

    process_users(uid_list)
    process_features(uid_list,time_list)
