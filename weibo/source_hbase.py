# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

from thrift.transport import *
from hbase import Hbase

from obj import *

_cfg = {
    'host': '192.168.9.12',
    'port': 9090,
    'table_user': 'sina_user',
    'table_status': 'sina_status',
}

decode_map = {
    str: None,
    int: '>i',
    long: '>q',
    bool: '>?'
}

pack_uid = lambda uid: buffer(struct.pack('<q', uid))
pack_mid = lambda uid,mid: buffer( struct.pack('<qq', uid, mid) )

def load_user(dic):
    profile = relation = status = tag = psy = None

    for col,val in dic.iteritems():
        c = bytearray(col)
        #寻找第一个:的位置，这里不应当使用split函数，否则如果qualifier当中含有的byte恰好等于:的ascii值的时候
        #就会被拆分成多个值而非两个
        splitter = c.find(':')
        cf = c[:splitter]
        qualifier = str( c[splitter+1:] )

        if cf == 'profile':
            t_type = UserProfile.types.get(qualifier)
            f = decode_map.get(t_type)

            if f is not None:
                v = struct.unpack(f, val.value)
                if isinstance(v,tuple): v=v[0]  # if unpacked value is tuple, pick the first element.
                if t_type==bool: v=bool(v)      # fix the bool bug, convert from int to bool.
            else:
                v = val.value

            if profile is None: profile = UserProfile()
            qualifier = str(qualifier)
            profile.setattr(qualifier,v)

        elif cf == 'relation':
            qualifier = struct.unpack('<q',qualifier)
            if relation is None: relation=dict()
            relation[qualifier] = val.value

        elif cf == 'status':
            qualifier = struct.unpack('<qq',qualifier)
            if status is None: status=dict()
            status[qualifier] = val.value

        elif cf == 'tag':
            if tag is None: tag=dict()
            tag[qualifier] = val.value

        elif cf == 'psy':
            if psy is None: psy=dict()
            psy[qualifier] = val.value

        else:
            raise ValueError('Unknown column family in UserLoader:[%s]!' % cf)

    return (profile,relation,status,tag,psy)


def load_status(dic):
    status = repost = None

    for col,val in dic.iteritems():
        c = bytearray(col)
        #寻找第一个:的位置，这里不应当使用split函数，否则如果qualifier当中含有的byte恰好等于:的ascci值的时候
        #就会被拆分成多个值而非两个
        splitter = c.find(':')
        cf = c[:splitter]
        qualifier = str( c[splitter+1:] )

        if cf=='status':
            t_type = Status.types.get(qualifier)
            f = decode_map.get(t_type)

            if f is not None:
                v = struct.unpack(f, val.value)
                if isinstance(v,tuple): v=v[0]  #if unpacked value is tuple, pick the first element.
                if t_type==bool: v=bool(v)      #fix the bool bug, convert from int to bool.
            else:
                v = val.value

            if status is None: status = Status()
            qualifier = str(qualifier)
            status.setattr(qualifier,v)

        elif cf=='repost':
            t_type = Repost.types[qualifier]
            f = decode_map.get(t_type)

            if f is not None:
                if t_type==long and len(val.value)==4: f='<i'
                v = struct.unpack(f, val.value)
                if isinstance(v,tuple): v=v[0]  #if unpacked value is tuple, pick the first element.
                if t_type==bool: v=bool(v)      #fix the bool bug, convert from int to bool.
            else:
                v = val.value

            if repost is None: repost = Repost()
            qualifier = str(qualifier)
            repost.setattr(qualifier,v)

    return (status,repost)


class DataSourceHBase:
    def __init__(self,cfg=None):
        self.cfg = _cfg if cfg is None else cfg
        self._transport = None

    def __del__(self):
        if self._transport:
            self._transport.close()

    def _get_client(self):
        if self._transport is None:
            self._transport = TTransport.TBufferedTransport(TSocket.TSocket(self.cfg['host'], self.cfg['port']))
            self._transport.open()

        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self._transport)
        client = Hbase.Client(protocol)

        return client

    def get_profile(self, uid):
        key = pack_uid(uid)

        client = self._get_client()
        results = client.getRowWithColumns(self.cfg['table_user'],key, ['relation','status','profile'],None)

        if len(results)>0:
            result = results[0]
            (profile,relation,status,tag,psy) = load_user(result.columns)
            return (profile,relation,status,tag,psy)
        else:
            return None


    def get_status(self, uid, mid):
        key = pack_mid(uid,mid)

        client = self._get_client()
        results = client.getRowWithColumns(self.cfg['table_status'],key, ['status','repost'],None)

        if len(results)>0:
            result = results[0]
            (status, repost) = load_status(result.columns)
            return (status, repost)
        else:
            return None


    def get_statuses(self, uid):
        key_beg = pack_mid(uid,0)
        key_end = pack_mid(uid,0x7fffffffffffffff)
        scan = Hbase.TScan(startRow=key_beg, stopRow=key_end)

        client = self._get_client()
        scanner = client.scannerOpenWithScan(self.cfg['table_status'], scan, None)

        i = 0
        while True:
            i += 1
            row_list = client.scannerGetList(scanner,i)
            if not row_list:
                break

            for row in row_list:
                yield load_status(row.columns)

        client.scannerClose(scanner)


if __name__ == '__main__':
    source = DataSourceHBase()
    #print source.get_profile(2474190592)[0]
    #print source.get_status(2474190592,3580543843428607)[0]
    i = 0
    for post, repost in source.get_statuses(2474190592): i+=1
    print i