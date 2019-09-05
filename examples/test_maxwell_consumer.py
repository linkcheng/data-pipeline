#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: start 
@date: 2019-06-03
export PYTHONPATH={pwd}
"""
import json
import logging

from operators.consumers.maxwell_consumer import MaxWellConsumer
from readers.kafka_reader import KafkaReader
from readers.mysql_reader import MySQLReader
from writers.mysql_writer import MySQLWriter
from utils.sql_builder import RowDataSQLBuilder
from utils.log import configure_logging

from config.db_config import CS_WR_CONFIG

configure_logging()
logger = logging.getLogger(__name__)

binaries = ['blob', 'binary', 'varbinary', ]
timestamps = ['timestamp', ]


class Message:
    def __init__(self, partition_id, offset, value):
        self.partition_id = partition_id
        self.offset = offset
        self.value = value


def maxwell_consumer_test():
    d, t = 'statistics', 'StarUser'
    c = """
    CREATE TABLE `StarUser1` (
      `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '用户id',
      `name` varchar(63) DEFAULT NULL COMMENT '用户姓名',
      `mobile` varchar(63) NOT NULL COMMENT '手机号',
      `gender` enum('male','female','other') NOT NULL DEFAULT 'other' COMMENT '性别',
      `person_id` bigint(20) unsigned NOT NULL DEFAULT '0',
      `utm_source` text,
      `created_time` datetime DEFAULT NULL COMMENT '用户创建时间',
      `updated_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (`id`),
      UNIQUE KEY `uidx_mobile` (`mobile`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cfg = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '12345678',
        'charset': 'utf8',
    }

    msgs = [
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-start",
            "ts": 1558339701,
            "data": {
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-insert",
            "ts": 1558339701,
            "data": {
                "id": 1,
                "name": 'hello',
                "mobile": "11111",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 01:01:01",
                "updated_time": "2019-05-18 01:01:01",
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-insert",
            "ts": 1558339701,
            "data": {
                "id": 2,
                "name": None,
                "mobile": "22222",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 02:02:02",
                "updated_time": "2019-05-18 02:02:02",
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-insert",
            "ts": 1558339701,
            "data": {
                "id": 3,
                "name": None,
                "mobile": "33333",
                "gender": "other",
                "person_id": 0,
                "utm_source": "大白信用分",
                "created_time": "2019-05-18 03:03:03",
                "updated_time": "2019-05-18 03:03:03",
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-insert",
            "ts": 1558339701,
            "data": {
                "id": 4,
                "name": None,
                "mobile": "44444",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 04:04:04",
                "updated_time": "2019-05-18 04:04:04",
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "bootstrap-complete",
            "ts": 1558339701,
            "data": {
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "insert",
            "ts": 1558339801,
            "xid": 34158,
            "commit": True,
            "data": {
                "id": 5,
                "name": None,
                "mobile": "55555",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 05:05:05",
                "updated_time": "2019-05-18 05:05:05",
            }
        },
        {
            "database": "v2",
            "table": "User",
            "type": "insert",
            "ts": 1558339901,
            "xid": 34258,
            "commit": True,
            "data": {
                "id": 6,
                "name": None,
                "mobile": "66666",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 06:06:06",
                "updated_time": "2019-05-18 06:06:06",
            }
        },
    ]

    umsgs = [
        {
            "database": "v2",
            "table": "User",
            "type": "update",
            "ts": 1559206362,
            "xid": 1032831,
            "commit": True,
            "data": {
                "id": 5,
                "name": None,
                "mobile": "55555",
                "gender": "other",
                "person_id": 5,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 05:05:05",
                "updated_time": "2019-05-20 15:05:05",
            },
            "old": {
                "person_id": 0,
                "updated_time": "2019-05-18 05:05:05",
            }
        },
        {
            "database": "v2",
            "table": "StarUser",
            "type": "update",
            "ts": 1559811068,
            "xid": 1032845,
            "commit": True,
            "data": {
                "id": 1,
                "name": 'hello',
                "mobile": "11111",
                "gender": "other",
                "person_id": 1,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 01:01:01",
                "updated_time": "2019-05-20 11:01:01",
            },
            "old": {
                "person_id": 0,
                "updated_time": "2019-05-18 01:01:01",
            }
        },
    ]

    dmsgs = [
        {
            "database": "v2",
            "table": "StarUser",
            "type": "delete",
            "ts": 1559811068,
            "xid": 2345789,
            "commit": True,
            "data": {
                "id": 3,
                "name": None,
                "mobile": "33333",
                "gender": "other",
                "person_id": 0,
                "utm_source": "大白信用分",
                "created_time": "2019-05-18 03:03:03",
                "updated_time": "2019-05-18 03:03:03",
            }
        },
        {
            "database": "v2",
            "table": "StarUser",
            "type": "delete",
            "ts": 1559811069,
            "xid": 2356789,
            "commit": True,
            "data": {
                "id": 1,
                "name": 'hello',
                "mobile": "11111",
                "gender": "other",
                "person_id": 0,
                "utm_source": "小白信用分",
                "created_time": "2019-05-18 01:01:01",
                "updated_time": "2019-05-18 01:01:01",
            }
        },
    ]

    mreader = MySQLReader(**CS_WR_CONFIG)

    columns_type = mreader.get_columns_type('cs', 'StarUser')
    ts = []
    bs = []
    for c, t in columns_type.items():
        if t in timestamps:
            ts.append(c)
        if t in binaries:
            bs.append(c)

    reader = KafkaReader('StarUser')
    builder = RowDataSQLBuilder(d, t)
    writer = MySQLWriter(builder, **CS_WR_CONFIG)
    consumer = MaxWellConsumer(reader, writer, timestamps=ts, binaries=bs)

    consumer.writer_thread.start()

    for i, msg in enumerate(msgs):
        msg = json.dumps(msg)
        msg = Message(0, i+1, msg)
        consumer.route(msg)

    # time.sleep(10)
    #
    # for i, msg in enumerate(umsgs):
    #     msg = json.dumps(msg)
    #     msg = Message(0, i+1, msg)
    #     consumer.route(msg)

    # time.sleep(10)

    # for i, msg in enumerate(dmsgs):
    #     msg = json.dumps(msg)
    #     msg = Message(0, i+1, msg)
    #     consumer.route(msg)
    consumer.writer_thread.join()


if __name__ == '__main__':
    maxwell_consumer_test()
