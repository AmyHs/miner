# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import codecs,json,os,re,numpy
from dateutil import parser

import nlpir
from textmind.wenxin import TextMind

from pyxll import xl_func

base_dir = r"J:/UserData/"

dir_profile = base_dir + r"Profile/"
dir_status  = base_dir + r"Status/"

re_exp = re.compile(r'\[(\S+?)\]')

# 第一人称复数 http://baike.baidu.com/view/1569657.htm
set_We = set(['我们','大家','咱们','咱们','俺们'])

# 第一人称单数
set_I = set(['我','俺'])

textLen = []

def get_lines(path_txt):
    lines = set()
    with codecs.open(path_txt, 'r', encoding='utf-8') as f:
        for line in f:
            e = line.strip(' \t\r\n')
            if len(e) > 0:
                lines.add(e)

    return lines

#Get absolute path of base dir
try:    ind = __file__.rindex('/' if '/' in __file__ else '\\')
except: ind = len(__file__)
cur_dir = __file__[:ind+1]

neg_exp = get_lines(cur_dir + r"./weibo/Exps-Neg.lst")
pos_exp = get_lines(cur_dir + r"./weibo/Exps-Pos.lst")

@xl_func("str uid, str attr: int",thread_safe=True, volatile=False)
def get_uattr(uid,attr):
    """returns specific attr for user with given uid"""
    fname = "%s%s.profile" % (dir_profile,uid)
    if not os.path.exists(fname):
        return "NoUser[%s]" % uid

    attrs = attr.split('/')
    with codecs.open(fname,'r',encoding='utf-8') as fp:
        profile = json.load(fp,encoding='utf-8')
        t = profile
        for a in attrs:
            if t is not None and isinstance(t,dict):
                t = t.get(a,'N/A[%s]-U%s' % (a,uid))
    return t

@xl_func("str uid, str attr: string",thread_safe=True, volatile=False)
def get_uattr_str(uid,attr):
    x = get_uattr(uid,attr)
    if isinstance(x,unicode):
        x = x.encode('gbk','ignore')
    return x


@xl_func("str uid, string date_filter: string[]",category="CCPL-Category",thread_safe=True, volatile=False)
def stat_statuses_text(uid, date_filter=None):
    fname = "%s%s.json" % (dir_status, uid)
    if not os.path.exists(fname):
        return [['N/A' for i in range(102)]]

    if date_filter is None: date_filter = 'Dec 12 23:59:59 +0800 2099'
    filter = parser.parse(date_filter,fuzzy=True)

    def iter_text(fname,filter):
        with codecs.open(fname,'r',encoding='utf-8') as fp:
            statuses = json.load(fp,encoding='utf-8')
            for s in statuses:
                if 'created_at' not in s:
                    continue

                created_at = s.get('created_at')
                created = parser.parse(created_at,fuzzy=True)
                if created > filter: continue

                text = s.get('text')
                yield text.encode('UTF-8')

    tm = TextMind()
    r = tm.process_iterator(iter_text(fname,filter))
    x = r.to_list()
    #print r.dump(to_ration=True,contains_header=True)
    return [['%s' % i for i in x]]


@xl_func("str uid, string date_filter: string[]",category="CCPL-Category", thread_safe=True, volatile=False)
def stat_statuses(uid, date_filter=None):
    fname = "%s%s.json" % (dir_status, uid)
    if not os.path.exists(fname):
        return [['N/A' for i in range(23)]]

    n = 0
    n_filtered = 0
    n_original = 0
    days = set()
    n_comments = 0.0
    n_repost = 0.0
    n_attitudes = 0.0
    n_url = 0
    n_pic = 0
    n_at = 0
    n_emotion = 0
    n_emotion_pos = 0
    n_emotion_neg = 0
    n_night = 0

    n_contains_I = 0
    n_contains_we = 0
    nTextLength = []

    if date_filter is None: date_filter = 'Dec 12 23:59:59 +0800 2099'
    filter = parser.parse(date_filter,fuzzy=True)

    minCreatedAt = parser.parse('Dec 31 23:59:59 +0800 2099',fuzzy=True)
    maxCreatedAt = parser.parse('Jan 01 00:00:01 +0800 1970',fuzzy=True)
    
    with codecs.open(fname,'r',encoding='utf-8') as fp:
        statuses = json.load(fp,encoding='utf-8')
        n = len(statuses)
        for s in statuses:
            if 'created_at' not in s:
                continue

            created_at = s.get('created_at')
            created = parser.parse(created_at,fuzzy=True)
            if created < minCreatedAt: minCreatedAt = created
            if created > maxCreatedAt: maxCreatedAt = created

            if created > filter: continue

            n_filtered += 1

            text = s.get('text')
            hour = created.hour

            n_original += 1 if "retweeted_status" in s else 0
            day = ''.join(str(t) for t in [created.year, created.month, created.day])
            days.add(int(day))
            n_comments += s.get('comments_count')
            n_repost += s.get('reposts_count')
            n_attitudes += s.get('attitudes_count')
            
            n_url += 1 if 'http://t.cn/' in text else 0
            n_pic += 1 if len(s['pic_ids'])>0 else 0
            n_at += sum([1 if i=='@' else 0 for i in text])

            l = len(text)
            nTextLength.append( l )
            if l==0:
                words = []
            else:
                t = text.encode('UTF-8')
                words = nlpir.Seg(t)
                for i in words:
                    word = i[0]
                    if word in set_I : n_contains_I += 1
                    if word in set_We : n_contains_we += 1


            exps = re.findall(re_exp,text)
            for exp in exps:
                n_emotion += 1
                if exp in neg_exp:
                    n_emotion_pos += 1
                elif exp in pos_exp:
                    n_emotion_neg += 1
            
            n_night += 1 if hour <6 or hour >22 else 0
            
    if n_filtered>0:
        n_comments /= n_filtered
        n_repost /= n_filtered
        n_attitudes /= n_filtered

    result = [n,
              n_filtered,
              n_original,
              n_pic,
              n_url,
              n_at,
              n_contains_we,
              n_contains_I,
              n_night,
              n_emotion,
              n_emotion_pos,
              n_emotion_neg,
              numpy.mean(nTextLength) if len(nTextLength)>0 else 'N/A',
              numpy.std(nTextLength) if len(nTextLength)>0 else 'N/A',
              numpy.max(nTextLength) if len(nTextLength)>0 else 'N/A',
              numpy.min(nTextLength) if len(nTextLength)>0 else 'N/A',
              numpy.median(nTextLength) if len(nTextLength)>0 else 'N/A',
              len(days),
              n_comments,
              n_repost,
              n_attitudes,
              minCreatedAt,
              maxCreatedAt
    ]
    return [[str(i) for i in result]]

def stat(uid):
    x = stat_statuses(uid)[0]
    y = stat_statuses_text(uid)[0]
    r1 = '\t'.join(x)
    r2 = '\t'.join(y)
    return r1 + '\t' + r2


if __name__ == '__main__':
    with codecs.open('E:/result.csv','w',encoding='utf-8') as fw:
        with codecs.open(u"E:/Study/Research-Suicide/Data-用户实验-v2/UserList.txt",'r',encoding='utf-8-sig') as fp:
            for line in fp:
                uid = line.strip(' \t\r\n')
                print uid
                r = stat(uid) + '\n'
                fw.write(r)


if __name__ == '__main__2':
    print stat(1191603864)