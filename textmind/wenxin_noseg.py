# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

'''
This file is for special use: in conditions where Chinese sentences are already segmented,
and both original paragraph and segmented paragraph are set as input.
'''

import re
import helper
from result import Result


def default_seg(string):  # whitespace_seg: use this function if paragraph are segmented already.
    for t in string.split():
        idx = t.rfind('\\')  # segmented word and POS separated by a char '\\'
        yield (t[:idx], t[1 + idx:])


sentence_separator = ['.', '。', '?', '？', '!', '！', ';', '；']  # 句号 问号 叹号 分号
sentence_separator = set([i.decode('utf-8') for i in sentence_separator])

rx_latin = re.compile("[^\W\d_]+", re.UNICODE)
rx_numeral = re.compile(r"([-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)", re.UNICODE)
rx_url = re.compile(
    r"((ht|f)tp(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-‌​\.\?\,\'\/\\\+&amp;%\$#_]*)?)",
    re.UNICODE)
rx_emotion = re.compile(r"(\[(.{1,16}?)\])", re.UNICODE)
rx_at_mention = re.compile(r"@[\u4e00-\u9fa5a-zA-Z0-9_-]{4,30} ", re.UNICODE)
rx_hashtag = re.compile(r"#[^#]+#", re.UNICODE)

is_sentence_separator = lambda term: term in sentence_separator
is_latin = lambda term: rx_latin.match(term) is not None
find_numeral = lambda string: [t[0] for t in rx_numeral.findall(string)]
find_url = lambda string: [t[0] for t in rx_url.findall(string)]
find_emotions = lambda string: [t[0] for t in rx_emotion.findall(string)]
find_at_mention = lambda string: [t[0] for t in rx_at_mention.findall(string)]
find_hashtag = lambda string: [t[0] for t in rx_hashtag.findall(string)]

# load dictionary
helper.load_all_dic()
get_term_tags = lambda term: helper.terms.get(term, list())


def process_iterator(lst_original, lst_segged, to_ration=True, enable_pos=False, encoding='utf-8'):
    """used to process list of paragraphs"""
    if len(lst_original) != len(lst_segged):
        raise ValueError('The length of lst_original and lst_segged should be equal!')

    r = Result()

    n_sentence_total_number = 1
    n_term_total_number = 0
    n_term_numerals = 0
    n_term_in_dic = 0
    n_term_len_gte6 = 0

    n_term_len_gte4 = 0
    n_term_latin = 0

    n_term_at_mention = 0
    n_term_emotion = 0
    n_term_hashtag = 0
    n_term_url = 0

    for line, seg_str in zip(lst_original, lst_segged):
        line = line.strip(' \t\r\n')  # .encode('utf-8')
        if len(line) < 1:
            continue

        n_term_numerals += len(find_numeral(line))
        n_term_at_mention += len(find_at_mention(line))
        n_term_emotion += len(find_emotions(line))
        n_term_hashtag += len(find_hashtag(line))
        n_term_url += len(find_url(line))

        segged_terms = default_seg(seg_str)
        for t in segged_terms:
            term = t[0].decode(encoding, 'ignore')
            pos = t[1]

            n_term_total_number += 1

            if is_sentence_separator(term):  # 如果是句子分隔符，句子数自增
                n_sentence_total_number += 1
            else:
                if is_latin(term): n_term_latin += 1
                if len(term) >= 6: n_term_len_gte6 += 1
                if len(term) >= 4: n_term_len_gte4 += 1

            tags = get_term_tags(term)
            if len(tags) > 0:
                n_term_in_dic += 1
                for tag in tags:
                    r.accumulate(tag)

            if enable_pos:
                r.accumulate('POS/%s' % pos)

    r.accumulate('stat/WordCount', value=n_term_total_number)
    if n_term_total_number == 0: n_term_total_number = float('NaN')

    r.accumulate('stat/WordPerSentence', value=float(n_term_total_number) / n_sentence_total_number)
    r.accumulate('stat/RateDicCover', value=float(n_term_in_dic) / n_term_total_number)
    r.accumulate('stat/RateNumeral', value=float(n_term_numerals) / n_term_total_number)
    r.accumulate('stat/RateSixLtrWord', value=float(n_term_len_gte6) / n_term_total_number)

    r.accumulate('stat/RateFourCharWord', value=float(n_term_len_gte4) / n_term_total_number)
    r.accumulate('stat/RateLatinWord', value=float(n_term_latin) / n_term_total_number)

    r.accumulate('stat/NumAtMention', value=n_term_at_mention)
    r.accumulate('stat/NumEmotion', value=n_term_emotion)
    r.accumulate('stat/NumHashTag', value=n_term_hashtag)
    r.accumulate('stat/NumURLs', value=n_term_url)

    return r.to_list(to_ration)


def process_paragraph(original_para, segged_para, to_ratio, encoding='utf-8', enable_pos=False):
    """used to process only one paragraph"""
    return process_iterator([original_para], [segged_para], to_ratio, encoding=encoding, enable_pos=enable_pos)


def get_header(enable_pos=True):
    return process_paragraph([''], [''], to_ratio=False, enable_pos=enable_pos).get_header()


if __name__ == '__main__':
    p = "Every dog has its own day. Big News: @解放日报 [最右]【呼市铁路局原副局长被判死缓 最头痛藏钱】2013年12月底，呼市铁路局原副局长马俊飞因受贿被判死缓。他说最头痛藏钱，从呼和浩特到北京，马俊飞又是购房又是租房，在挥之不去的恐惧中，人民币8800万、美元419万、欧元30万、港币27万，黄金43.3公斤，逐渐堆满了两所#房子#…… http://t.cn/8kgR6Yi"
    p_segged = r"Every\ws dog\v has\ws its\ws own\n day\ws .\wp Big\ws News\ws :\wp @\v 解放\v 日报\n [\wp 最\d 右\nd ]\v 【\ns 呼市\ns 铁路局\n 原\b 副\b 局长\n 被\p 判\v 死缓\j 最\d 头痛\a 藏\v 钱\n 】\wp 2013年\nt 12月\nt 底\nd ，\wp 呼市\ns 铁路局\n 原\b 副\b 局长\n 马俊飞\nh 因\p 受贿\v 被\p 判\v 死缓\j 。\wp 他\r 说\v 最\d 头痛\a 藏\v 钱\n ，\wp 从\p 呼和浩特\ns 到\p 北京\ns ，\wp 马俊飞\nh 又\d 是\v 购房\v 又\d 是\v 租房\n ，\wp 在\p 挥之不去\i 的\u 恐惧\a 中\nd ，\wp 人民币\n 8800万\m 、\wp 美元\n 419万\m 、\wp 欧元\n 30万\m 、\wp 港币\n 27万\m ，\wp 黄金\n 43.3\m 公斤\q ，\wp 逐渐\d 堆满\v 了\u 两\m 所\q #\v 房子\n #\n ……\wp http://t.cn/8kgR6Yi\%"
    r = process_paragraph(p, p_segged, to_ratio=True)
    print '\t'.join([str(t) for t in r])
