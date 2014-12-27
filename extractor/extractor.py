# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import re,numpy
from dateutil import parser
from pytz import UTC as utc
from collections import OrderedDict

import nlpir,util
from textmind.wenxin import TextMind

re_exp = re.compile(r'\[(\S+?)\]')

set_We = set(['我们','大家','咱们','咱们','俺们'])    # 第一人称复数 http://baike.baidu.com/view/1569657.htm
set_I  = set(['我','俺'])                             # 第一人称单数

textLen = []

#Get absolute path of base dir
try:    ind = __file__.rindex('/' if '/' in __file__ else '\\')
except: ind = len(__file__)
cur_dir = __file__[:ind+1]

neg_exp = util.readlines(cur_dir + r"./weibo/Exps-Neg.lst")
pos_exp = util.readlines(cur_dir + r"./weibo/Exps-Pos.lst")

def iter_text(_statuses,_filter):
    for s in _statuses:
        if _filter is not None:
            if 'created_at' not in s:
                continue
            else:
                created_at = s.get('created_at')
                created = parser.parse(created_at,fuzzy=True) if isinstance(created_at,basestring) else utc.localize(created_at)
                if created > _filter: continue

        text = s.get('text').encode('UTF-8')
        yield text


def extract_statuses_text(statuses, date_filter='Dec 12 23:59:59 +0800 2099'):
    date_filter = parser.parse(date_filter,  fuzzy=True)
    tm = TextMind()
    iter = iter_text(statuses, date_filter)
    r = tm.process_iterator(iter)
    x = r.to_list()
    # print r.dump(to_ration=True,contains_header=True)
    return ['%s' % i for i in x]


def extract_statuses_behave(statuses, date_filter=None):
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
    
    for s in statuses:
        n += 1
        if 'created_at' not in s:
            continue

        created_at = s.get('created_at')
        created = parser.parse(created_at,fuzzy=True) if isinstance(created_at,basestring) else utc.localize(created_at)
        if created < minCreatedAt: minCreatedAt = created
        if created > maxCreatedAt: maxCreatedAt = created

        if created > filter: continue

        n_filtered += 1

        text = s.get('text')
        hour = created.hour

        n_original += 1 if "retweeted_status" in s or s.get('is_original', 0) == 1 else 0
        day = ''.join(str(t) for t in [created.year, created.month, created.day])
        days.add(int(day))
        n_comments += s.get('comments_count',0)
        n_repost += s.get('reposts_count',0)
        n_attitudes += s.get('attitudes_count',0)

        n_url += 1 if 'http://t.cn/' in text else 0
        n_pic += 1 if len(s.get('pic_ids',s.get('original_pic',0))  )>0 else 0
        n_at += sum([1 if i=='@' else 0 for i in text])

        l = len(text)
        nTextLength.append( l )
        words = []
        if l>0:
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

    if n<1:
        return ['N/A' for i in range(23)]

    if n_filtered>0:
        n_comments /= n_filtered
        n_repost /= n_filtered
        n_attitudes /= n_filtered

    result = [n,
              n_filtered,       #公开微博总数
              n_original,       #原创微博数'
              n_pic,            #含图片原创微博数'
              n_url,            #含URL微博数
              n_at,             #含@的微博数
              n_contains_we,    #微博中第一人称复数使用次数
              n_contains_I,     #微博中第一人称单数使用次数
              n_night,          #夜间时段发微博数
              n_emotion,        #含表情总数
              n_emotion_pos,    #含积极表情总数
              n_emotion_neg,    #含消极表情总数
              numpy.mean(nTextLength) if len(nTextLength)>0 else 'N/A',      #公开微博字数平均值
              numpy.std(nTextLength) if len(nTextLength)>0 else 'N/A',       #公开微博字数STD
              numpy.max(nTextLength) if len(nTextLength)>0 else 'N/A',       #公开微博字数MAX
              numpy.min(nTextLength) if len(nTextLength)>0 else 'N/A',       #公开微博字数MIN
              numpy.median(nTextLength) if len(nTextLength)>0 else 'N/A',    #公开微博字数MEDIAN
              len(days),        #发微博天数
              n_comments,       #评论数
              n_repost,         #转发数
              n_attitudes,      #表态数
              minCreatedAt,     #最早一条微博发布时间
              maxCreatedAt      #最后一条微博发布时间
    ]
    return [str(i) for i in result]


def extract_profile(u):
    f = OrderedDict()
    f['Nick'] = u.get('screen_name')
    f['性别'] = u.get('gender')
    f['所在地'] = u.get('location')
    f['允许所有人发送私信'] = '1' if u.get('allow_all_act_msg') else '0'
    f['允许所有人评论'] = '1' if u.get('allow_all_comment') else '0'
    f['有自定义头像'] = '1' if '180/0/' in u.get('avatar_large') else '0'
    f['互粉数/粉丝数'] = str(1.0 * u.get('bi_followers_count') / u.get('followers_count')) if u['followers_count']>0 else 'DIV#0'
    f['互粉数/关注数'] = str(1.0 * u.get('bi_followers_count') / u.get('friends_count')) if u['friends_count']>0 else 'DIV#0'
    f['互粉数'] = str(u.get('bi_followers_count'))

    created_at = u.get('created_at')
    f['开博日期'] = str( parser.parse(created_at,fuzzy=True).date() if isinstance(created_at,str) else created_at)
    f['自我描述长度'] = str( len( u.get('description'))  )
    f['自我描述中含“我”'] = '1' if u'我' in u.get('description') else '0'
    f['个性域名'] = u.get('domain')
    f['域名长度'] = str( len( u.get('domain') ))
    f['域名中含数字'] = '1' if len([val for val in u.get('domain') if val in '0123456789'])>0 else '0'
    f['微博数'] = str(u.get('statuses_count'))
    f['收藏数'] = str(u.get('favourites_count'))
    f['粉丝数'] = str( u.get('followers_count') )
    f['关注数'] = str( u.get('friends_count') )
    f['开启地理定位'] = '1' if u.get('geo_enabled') else '0'
    f['用户个人网站URL含域名'] = '1' if u.get('domain') in u.get('url') else '0'
    f['昵称长度'] = str( len( u.get('screen_name') ) )
    f['用户有个人网站URL'] = '1' if u.get('url') is not None else '0'
    f['是否认证'] = '1' if u.get('verified') else '0'
    f['认证原因长度'] = str( len ( u.get('verified_reason') ) )
    f['认证类别'] = str(u.get('verified_type'))
    f['省/市ID'] = u.get('province')
    f['市/区ID'] = u.get('city')
    return [i if not isinstance(i,(int, long, float)) else str(i) for i in f.values()]

def stat(statuses):
    x = extract_statuses_behave(statuses)
    y = extract_statuses_text(statuses)
    x.extend(y)
    return x
    #return '\t'.join(x)
