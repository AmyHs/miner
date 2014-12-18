#encoding=utf-8
__author__ = 'Peter'

from weibo.util import time2epoch as time2epoch
from weibo.util import time2str as time2str
from weibo.util import time_format as time_format
from weibo.util import now2epoch as now2epoch
from weibo.util import get_mtime as get_mtime

import codecs
def readlines(fpath,encode='utf-8-sig'):
    with codecs.open(fpath,'r',encoding=encode) as fp:
        results = []
        for line in fp:
            line = line.strip(' \t\r\n')
            if len(line)>0:
                results.append(line)
    return results