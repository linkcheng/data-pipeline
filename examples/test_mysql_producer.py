#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mysql_producer 
@date: 2019-07-06
export PYTHONPATH={pwd}
"""

import logging

from readers.mysql_reader import MySQLReader
from writers.kafka_writer import KafkaWriter
from operators.producers.mysql_producer import MySQLProducer

from utils.log import configure_logging
from config.db_config import MYSQL_R_CONFIG

configure_logging()
logger = logging.getLogger(__name__)


def mysql_producer_test():
    tbs = ['statistics.StarUser1', 'statistics.StarUser2']

    writer = KafkaWriter()
    reader = MySQLReader(tbs, **MYSQL_R_CONFIG)

    producer = MySQLProducer(reader, writer)
    producer.run()


if __name__ == '__main__':
    mysql_producer_test()
