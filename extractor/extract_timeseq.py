# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import os
import codecs
import json
from collections import defaultdict,OrderedDict
from datetime import datetime

import util
from textmind.wenxin import TextMind


# 特征提取数据导出目录
base_dir = 'G:/Exp-02/'

# from source_mysql import DataSourceMySQL as DataSource
from weibo.source_fjson import DataSourceFJson as DataSource

# 设定数据源
dSource = DataSource(base_dir)


def get_statuses_groupby_week(uid):
    fields = ['created_at', 'text']
    result = defaultdict(str)
    for s in dSource.get_statuses(uid, fields):
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


def process_user(uid):
    u_result = {}

    try:
        with codecs.open('%sOriginal/%s.json' % (base_dir, uid), 'w', encoding='utf-8') as fp:
            statuses = get_statuses_groupby_week(uid)
            keys = sorted(statuses.iterkeys())
            for k in keys:
                textMind = TextMind()
                t = statuses.get(k)
                v = t.encode('utf-8')

                if len(v) > 0:
                    vec = textMind.process_paragraph(v,encoding='utf-8').dump(separator=',')
                    u_result[k] = vec

            json.dump(u_result, fp, indent=1, sort_keys=True)
    except Exception as e:
        print(uid)
        print(e)


def process_features(uid_list, time_list):
    header = TextMind().get_header()

    fw_pool = OrderedDict()
    for head in header:
        k = head.replace('/','_')
        v = '%sTimeSequence/%s.csv' % (base_dir, k)
        fp = open(v, 'w+')
        fw_pool[head] = fp

    u_data = OrderedDict()

    for uid in uid_list:
        fname = '%sOriginal/%s.json' % (base_dir, uid)
        if not os.path.exists(fname):
            continue

        with codecs.open(fname, 'r', encoding='utf-8-sig') as fp:
            u_seq = json.load(fp, encoding='utf-8')

            # 判断时序数据是否充足，可用于进行分析
            tmp_counter = [1 if t in u_seq else 0 for t in time_list]
            if sum(tmp_counter) < 40:
                continue

            times = set()
            for t in time_list:
                vec = u_seq.get(t, None)
                features = [0] * len(header) if vec is None else vec.split(',')
                if vec is not None:
                    times.add(t)

                idx = 0
                for head, fw in fw_pool.iteritems():
                    fw.write('%s,' % features[idx])
                    idx += 1

            for fw in fw_pool.values():
                fw.write('\n')

        u_data[uid] = times

    with codecs.open('%ssummary.csv' % base_dir, 'w', encoding='utf-8') as fp:
        fp.write(',')
        for t in time_list:
            fp.write('%s,' % t)
        fp.write('\n')

        for uid,times in u_data.iteritems():
            fp.write('%s,' % uid)
            for t in time_list:
                cell = 1 if t in times else 0
                fp.write('%s,' % cell)
            fp.write('\n')


if __name__ == '__main__':
    # from multiprocessing import Pool, freeze_support
    # pool = Pool()
    # freeze_support()

    uid_list = util.readlines(base_dir + "UserList.txt")

    # [process_user(uid) for uid in uid_list[400:500]]
    # pool.map(process_user, uid_list)
    # pool.close()

    time_list = ['2011%02d' % i for i in range(1, 53)] + ['2012%02d' % i for i in range(1, 42)]
    process_features(uid_list, time_list)
