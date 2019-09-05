#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mongo_consumer 
@date: 2019-06-29
export PYTHONPATH={pwd}
"""
import logging

from readers.kafka_reader import KafkaReader
from writers.mongo_writer import MongoWriter
from operators.consumers.mongo_consumer import MongoDBConsumer

from utils.log import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


def mongo_consumer_test():
    d, t = 'rcs_gateway', 'baofuLog'
    config = {
        'host': 'localhost',
        'port': 27017,
        'user': 'reporter',
        'password': '12345678',
    }

    writer = MongoWriter(d, t, **config)
    reader = KafkaReader(f'{d}-{t}')

    consumer = MongoDBConsumer(reader, writer)
    consumer.run()


if __name__ == '__main__':
    mongo_consumer_test()
