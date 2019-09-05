#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mongo_connector 
@date: 2019-06-13 
"""
from urllib.parse import quote_plus as qp
from pymongo import MongoClient


class MongoDBConnector:
    def __init__(self, user, password, host, port, database=None):
        # mongodb://[username:password@]host1[:port1][,host2[:port2],…[,hostN[:portN]]][/[database][?options]]
        self.uri = f'mongodb://{qp(user)}:{qp(password)}@{host}:{port}/'
        self.database = database
        if self.database:
            self.uri += self.database

        self.client = MongoClient(self.uri)

    def watch(self, *args, **kwargs):
        return self.client.watch(*args, **kwargs)

    def close(self):
        self.client.close()


if __name__ == '__main__':
    pass
