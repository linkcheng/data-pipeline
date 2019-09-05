#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mongo_producer 
@date: 2019-06-16
export PYTHONPATH={pwd}
"""
import logging

from readers.mongo_reader import MongoDBReader
from writers.kafka_writer import KafkaWriter
from operators.producers.mongo_producer import MongoDBProducer

from utils.log import configure_logging
from config.db_config import MONGO_R_CONFIG

configure_logging()
logger = logging.getLogger(__name__)


def mongo_producer_test():
    tbs = ['rcs_gateway.baofuLog']

    writer = KafkaWriter()
    reader = MongoDBReader(tbs, **MONGO_R_CONFIG)

    producer = MongoDBProducer(reader, writer)
    producer.run()


if __name__ == '__main__':
    mongo_producer_test()
