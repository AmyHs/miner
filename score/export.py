# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import sys,codecs
from collections import OrderedDict
from datetime import datetime
import dbutil,meta


dimensions = OrderedDict()
users = dict()

quiz_with_answer = ['BJ_Demographic']

def get_exp_fills(ExpId):
    cur = dbutil.get_cur()
    cur.execute('CALL P_GetExpFills(%s,null,null)' % ExpId)
    for r in cur:
        sina_uid = r['SiteUid']
        quiz_id = r['QuizId']
        costseconds = r['CostSeconds']
        filltime = r['LastFill']
        answer = r['Answer']
        score = r['Score']

        quiz = meta.get_quiz(quiz_id)
        ss = quiz.score(answer,col_name=quiz_id)

        u = users.get(sina_uid)
        if u is None:
            u = OrderedDict()
        u.update(ss)
        for s in ss:
            dimensions[s] = 0

        if quiz_id in quiz_with_answer:
            answers = quiz.parse(answer, col_name=quiz_id)
            u.update(answers)
            for a in answers:
                dimensions[a] = 0

        dim_cost_seconds = '%s_CostSeconds' % quiz_id
        dim_fill_time = '%s_FillTime' % quiz_id

        u[dim_cost_seconds] = costseconds
        u[dim_fill_time] = datetime.strftime(filltime,'%Y/%m/%d %H:%M:%S')

        dimensions[dim_cost_seconds] = 0
        dimensions[dim_fill_time] = 0

        users[sina_uid] = u


if __name__ == '__main__':
    exp_id = 0

    print sys.argv

    if len(sys.argv)<=1:
        print('Usage: python export.py exp_id [output_file_path]')
        exit()

    if len(sys.argv)>1:
        exp_id = int(sys.argv[1])

    if len(sys.argv)>2:
        fpath = sys.argv[2]
        try:
            fp = codecs.open(fpath, 'w', 'utf-8')
            fp.write(u'\uFEFF')
            sys.stdout = fp
        except:
            raise RuntimeError('Unable to open file [%s] to write.' % fpath)

    get_exp_fills(exp_id)

    header = "SinaUid,"
    for dim in dimensions:
        header += "%s," % dim
    print header

    for u,scores in users.iteritems():
        line = '%s,' % u
        for dim in dimensions:
            line += '%s,' % scores.get(dim,'')

        print line