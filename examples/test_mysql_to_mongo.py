#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mysql_to_mongo 
@date: 2019-07-16
export PYTHONPATH={pwd}
"""
import logging

from connectors.mysql_connector import MySQLConnector
from readers.kafka_reader import KafkaReader
from writers.mongo_writer import MongoWriter
from operators.consumers.mysql_consumer import MySQLConsumer

from config.db_config import MYSQL_R_CONFIG
from utils.log import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

binaries = ['blob', 'binary', 'varbinary', ]


def mongo_consumer_test():
    d = 'statistics'
    t = 'StarUser2'
    config = {
        'host': 'localhost',
        'port': 27017,
        'user': 'reporter',
        'password': '12345678',
    }

    mc = MySQLConnector(**MYSQL_R_CONFIG)
    columns_type = mc.get_columns_type(d, t)
    bs = [c for c, t in columns_type.items() if t in binaries]
    logger.info(bs)
    mc.close()

    writer = MongoWriter(d, t, **config)
    reader = KafkaReader(f'{d}-{t}', group_id='group-2')

    consumer = MySQLConsumer(reader, writer, binaries=bs)
    consumer.run()


if __name__ == '__main__':
    mongo_consumer_test()
