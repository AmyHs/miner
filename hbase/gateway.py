# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

import Hbase
from ttypes import *
from thrift.transport import *

port = 9090
hosts = ['192.168.9.%d' % i for i in range(1,45)]

class HBaseClient:
    def __init__(self, index=0, ):
        address = HBaseClient.cfg[index]['host']
        port = HBaseClient.cfg[index]['port']
        self.index = index
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        self.client = Hbase.Client(self.protocol)
        self.transport.open()

    def reconnect(self):
        try:
            self.transport.close()
        except Exception as e:
            print e

        maxTry = 100
        while True:
            maxTry -= 1
            if maxTry < 0:
                exit(1)
            try:
                self.index = (self.index+1)% len( HBaseClient.cfg )
                address = HBaseClient.cfg[self.index]['host']
                port = HBaseClient.cfg[self.index]['port']

                self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))
                self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

                self.client = Hbase.Client(self.protocol)
                self.transport.open()
                break
            except Exception as e:
                print 'reconnect error:', e.message

    def clientClose(self):
        self.transport.close()

    def applyBatch(self, batches):
        for batch in batches:
            tableName = batch['tableName']
            rowBatches = batch['rowBatches']

            retryTime = 3
            while retryTime>0:
                retryTime -= 1
                try:
                    self.client.mutateRows(tableName,[rowBatches],None)
                    break
                except Exception as e:
                    if retryTime == 0:
                        self.reconnect()
                        retryTime = 3

HBaseClient.cfg = [{'host':host, 'port':port} for host in hosts]