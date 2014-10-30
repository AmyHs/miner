#encoding=utf-8
__author__ = 'Peter'

from weibo.util import time2epoch as time2epoch
from weibo.util import time2str as time2str
from weibo.util import time_format as time_format
from weibo.util import now2epoch as now2epoch
from weibo.util import get_mtime as get_mtime

import codecs
def readlines(fpath,encode='utf-8-sig'):
    return [line.strip(' \t\r\n') for line in codecs.open(fpath,'r',encoding=encode) if len(line.strip(' \t\r\n'))>0]