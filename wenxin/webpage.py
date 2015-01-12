# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import web
import json
import sqlite3 as db
from textmind.wenxin import process_paragraph

def register(d):
    con = None
    try:
        con = db.connect('register.db')
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS "register"(
            "id"  INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            "name"  TEXT NOT NULL,
            "email"  TEXT NOT NULL,
            "affiliation"  TEXT NOT NULL,
            "ipaddr"  TEXT NOT NULL,
            "time"  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            "message"  TEXT NOT NULL
        )''')
        cur.execute("INSERT INTO register (Name, Email, Affiliation, Message, IPAddr) VALUES (?,?,?,?,?)",
            (d['name'], d['email'], d['affiliation'], d['message'], d['ip'])
        )
        con.commit()
    except Exception as e:
        raise e
    finally:
        con and con.close()

# Url mappings
urls = (
    '/', 'Index',
    '/analysis', 'Analysis',
    '/register', 'Register',
)

### Templates
render = web.template.render('templates', base='base')

class Index:
    def GET(self):   # Show page
        return render.index()

class Analysis:
    def POST(self):  # Process Text
        result = dict()

        try:
            data = web.input()
            string = data.get('str').encode('utf-8')
            hist = process_paragraph(string,enable_pos=False).to_dict(to_ratio=False)
            hist = [{'name':k, 'value':v} for k, v in hist.iteritems()]
            result['result'] = hist
            result['status'] = 'success'
        except Exception as e:
            result['msg'] = e.message
            result['status'] = 'fail'

        web.header('Content-Type', 'application/json')
        return json.dumps(result)

class Register:
    def POST(self):
        result = dict()
        fields = {'name':None,'email':None,'affiliation':None,'message':None}
        try:
            form = web.input()
            for field in fields:
                string = form.get(field)#.encode('utf-8')
                if len(string)>100:
                    raise ValueError("Field information to long for %s!" % field)
                fields[field] = string
            #ip = web.ctx.get('ip','UNKNOWN')
            ip = web.ctx.get('environ',{}).get('HTTP_X_FORWARDED_FOR','UNKNOWN')
            fields['ip'] = ip.split(':')[0]
            register(fields)

            result['url'] = 'http://pan.baidu.com/s/1o6l0g0y'
            result['status'] = 'success'
        except Exception as e:
            result['status'] = 'fail'
            result['msg'] = e.message

        web.header('Content-Type', 'application/json')
        return json.dumps(result)

web.config.debug = False
app = web.application(urls, globals())

if __name__ == '__main__':
    app.run()