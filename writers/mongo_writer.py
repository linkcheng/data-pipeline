#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mongo_writer 
@date: 2019-06-24 
"""
import logging
from collections import deque

from writers.base import BaseWriter
from connectors.mongo_connector import MongoDBConnector

logger = logging.getLogger(__name__)


class MongoWriter(BaseWriter):
    def __init__(self, db_name, coll_name, *, user, password, host, port,
                 database=None):
        self.conn = MongoDBConnector(user=user, password=password,
                                     host=host, port=port, database=database)
        self.client = self.conn.client

        self.db_name = db_name
        self.coll_name = coll_name
        self.db = self.client[db_name]
        self.coll = self.db[coll_name]

        self.fun_args = deque(maxlen=self.maxlen)

    def write(self, fun, *args, **kwargs):
        """\
        保存函数与参数
        :param fun: 函数
        :param args: 位置参数
        :return:
        """
        # insert 函数参数可以合并一起执行
        if fun.__name__ == 'insert_many':
            if self.fun_args and self.fun_args[-1][0] == fun:
                f, ((a, ), k) = self.fun_args[-1]
                a.extend(args)
                k.update(kwargs)

                if len(a) >= self.maxlen:
                    self.commit()
            else:
                self.fun_args.append((fun, ((deque(args), ), kwargs)))
        else:
            self.fun_args.append((fun, (args, kwargs)))

        if len(self.fun_args) >= self.maxlen:
            self.commit()

    def commit(self):
        """提交，真正执行 fun"""
        for fun, (args, kwargs) in self.fun_args:
            fun(*args, **kwargs)
        self.fun_args.clear()

    def insert(self, full_documents: dict, db_name=None, coll_name=None,
               *args, **kwargs):
        if db_name and coll_name:
            coll = self.client[db_name][coll_name]
        else:
            coll = self.coll
        # coll.insert_many(full_documents)
        self.write(coll.insert_many, full_documents)
        return self.client[db_name][coll_name].insert_many, full_documents

    def update(self, full_document: dict, document_key: dict,
               db_name=None, coll_name=None, *args, **kwargs):
        """update or insert data
        :param full_document: 完全数据
        :param document_key: 过滤条件
        :param db_name: 要写入的 db_name
        :param coll_name: 要写入的 coll_name
        """
        if db_name and coll_name:
            coll = self.client[db_name][coll_name]
        else:
            coll = self.coll
        # coll.update_one(document_key, full_document, upsert=True)
        self.write(coll.update_one, document_key, {'$set': full_document},
                   upsert=True)
        return coll.update_on, (document_key, {'$set': full_document})

    def delete(self, document_key: dict, db_name=None, coll_name=None,
               *args, **kwargs):
        if db_name and coll_name:
            coll = self.client[db_name][coll_name]
        else:
            coll = self.coll
        # coll.delete_one(document_key)
        self.write(coll.delete_one, document_key)
        return coll.delete_one, document_key

    def stop(self):
        super().stop()
        self.conn.close()
        logger.info(f'MongoDB writer [{self.db_name}.{self.coll_name}] stopped')


if __name__ == '__main__':
    from bson import ObjectId
    d, t = 'reporting', 'bizEventReport2019'
    config = {
        'host': 'localhost',
        'port': 27017,
        'user': 'reporter',
        'password': '12345678',
    }
    mw = MongoWriter(d, t, **config)

    i0 = {
        "event_time": "1561279598302",
        "source_type": "client",
        "device_id": "20190623fabe8cd7d",
        "session_id": "f94fd3feb6e1c98c9081df265fcab8da",
        "event_id": "0000000",
        "user_id": "0000000",
        "is_login": 1,
        "mobile": "15200000000",
        "token": "f94fd3feb6e1c98c9081df265fcab8da",
        "os": "ios",
        "app": "xyf",
        "channel": "ios:appstore",
        "version_code": "00000",
        "utm_source": "",
        "created_time": "2019-06-23 16:46:40"
    }
    i1 = {
        "event_time": "1561279598302",
        "source_type": "client",
        "device_id": "20190623fabe8cd7d",
        "session_id": "f94fd3feb6e1c98c9081df265fcab8da",
        "event_id": "1111111",
        "user_id": "1111111",
        "is_login": 1,
        "mobile": "15211111111",
        "token": "f94fd3feb6e1c98c9081df265fcab8da",
        "os": "ios",
        "app": "xyf",
        "channel": "ios:appstore",
        "version_code": "11111",
        "utm_source": "",
        "created_time": "2019-06-23 16:46:40"
    }

    # mw.insert(i0)
    # mw.insert(i1)
    # mw.commit()

    u0 = {
        "_id" : ObjectId("5d172db2d578c8aee861d0c9"),
        "event_time" : "1561279598302",
        "source_type" : "client",
        "device_id" : "20190623fabe8cd7d",
        "session_id" : "f94fd3feb6e1c98c9081df265fcab8da",
        "event_id" : "0000000",
        "user_id" : "0000000",
        "is_login" : 1,
        "mobile" : "15200000000",
        "token" : "f94fd3feb6e1c98c9081df265fcab8da",
        "os" : "ios",
        "app" : "xyf",
        "channel" : "ios:appstore",
        "version_code" : "00000",
        "utm_source" : "",
        "created_time" : "2019-06-23 16:46:40"
    }
    dk0 = {
        "_id": ObjectId("5d172db2d578c8aee861d0c9"),
    }
    mw.upsert(dk0, u0)
    mw.commit()
    #
    # mw.delete(dk0)
    # mw.commit()
