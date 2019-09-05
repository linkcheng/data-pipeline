#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: confluent_kafka_reader
@date: 2019-07-13
@doc:
https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent-kafka-confluent-s-python-client-for-apache-kafka
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
"""
import logging
from confluent_kafka import Consumer, OFFSET_INVALID

from readers.mq_base import MQReader
from utils.str_utils import b2s
from config.sys_config import (
    BOOTSTRAP_SERVERS,
    BROKER_VERSION,
    COMPRESSION_TYPE
)

logger = logging.getLogger(__name__)


class KafkaReader(MQReader):
    @staticmethod
    def decode(args):
        k, v = args
        return b2s(k), int(v)

    @staticmethod
    def get_value(msg):
        return msg.value()

    @staticmethod
    def get_token(msg):
        return {f'{msg.topic()}.{msg.partition()}': msg.offset() + 1}

    def __init__(self, topics, group_id='group-1', client_id='default',
                 bootstrap_servers=BOOTSTRAP_SERVERS,
                 is_bootstrap=True, is_resume=True):
        """从 Kafka 读取数据
        :param topics: list, kafka topics
        :param group_id: str, kafka topics group_id
        :param client_id: str, kafka topics client_id
        :param bootstrap_servers: kafka host
        :param is_bootstrap: 是否全量读取
        :param is_resume: 是否断点续传
        """
        if not isinstance(topics, (list, tuple)):
            topics = [topics]
        self.topics = topics
        self.group_id = group_id
        self.client_id = client_id

        super().__init__(f'kafka:{self.client_id}:{self.group_id}',
                         is_bootstrap=is_bootstrap, is_resume=is_resume)

        self.config = {
            'client.id': self.client_id,
            'group.id': self.group_id,
            'bootstrap.servers': bootstrap_servers,
            'broker.version.fallback': BROKER_VERSION,
            'compression.type': COMPRESSION_TYPE,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest' if is_bootstrap else 'latest',
            'on_commit': self.on_commit,
        }
        self.consumer = Consumer(self.config)

    def on_commit(self, err, partitions):
        for part in partitions:
            if err is not None:
                logger.error(f'Message delivery failed: {err}')
                logger.error(
                    f'topic={part.topic} partition={part.partition} '
                    f'offset={part.offset}'
                )
            else:
                # logger.info(
                #     f'topic={part.topic} partition={part.partition} '
                #     f'offset={part.offset}'
                # )
                key = f'{part.topic}.{part.partition}'

                if part.offset != OFFSET_INVALID:
                    self.resume_token[key] = part.offset

    def read(self):
        """从 kafka 读取数据
        https://github.com/confluentinc/confluent-kafka-python/issues/201
        :return:
        """
        # 重置 offset
        def on_assign(consumer, partitions):
            consumer.assign(partitions)
            for part in partitions:
                key = f'{part.topic}.{part.partition}'
                part.offset = self.resume_token.get(key) or 0
            consumer.commit(offsets=partitions, asynchronous=False)

        # 取消订阅
        if self.is_resume:
            self.consumer.subscribe(self.topics, on_assign=on_assign)
        else:
            self.consumer.subscribe(self.topics)
        logger.info(f'Kafka consumer subscribe {self.topics}')

        while True:
            try:
                msg = self.consumer.poll(1)
            except RuntimeError as e:
                logger.error(f'RuntimeError:{e}')
                break
            except KeyboardInterrupt:
                logger.error('KeyboardInterrupt')
                break

            if msg is None:
                continue
            if msg.error():
                logger.error(msg.error())
                continue

            # logger.info(f'{msg.topic()} {msg.partition()} {msg.offset()}')
            yield msg

        # 取消订阅
        try:
            self.consumer.unsubscribe()
        except RuntimeError:
            pass
        logger.info(f'Kafka reader {self.topics} unsubscribe')

    def disconnect(self):
        try:
            self.consumer.close()
        except RuntimeError:
            pass


if __name__ == '__main__':
    from utils.log import configure_logging
    configure_logging()

    tps = ['statistics.StarUser1', 'statistics.StarUser2']
    r = KafkaReader(tps)
    try:
        for m in r.read():
            logger.info(f'{m.topic()} {m.partition()} {m.offset()} {m.value()}')
    finally:
        r.stop()
