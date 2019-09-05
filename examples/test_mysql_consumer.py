#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mysql_consumer 
@date: 2019-07-15
export PYTHONPATH={pwd}
"""
import logging

from readers.kafka_reader import KafkaReader
from connectors.mysql_connector import MySQLConnector
from writers.mysql_writer import MySQLWriter
from operators.consumers.mysql_consumer import MySQLConsumer

from utils.log import configure_logging
from config.db_config import CS_WR_CONFIG, MYSQL_R_CONFIG

configure_logging()
logger = logging.getLogger(__name__)

binaries = ['blob', 'binary', 'varbinary', ]


def mysql_consumer_test():
    d, t = 'statistics', 'StarUser2'

    mc = MySQLConnector(**MYSQL_R_CONFIG)
    columns_type = mc.get_columns_type(d, t)
    bs = [c for c, t in columns_type.items() if t in binaries]
    logger.info(bs)
    mc.close()

    writer = MySQLWriter(d, t, **CS_WR_CONFIG)
    reader = KafkaReader(f'{d}-{t}')

    consumer = MySQLConsumer(reader, writer, binaries=bs)
    consumer.run()


if __name__ == '__main__':
    mysql_consumer_test()
