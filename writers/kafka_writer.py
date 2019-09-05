#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: confluent_kafka_writer
@date: 2019-07-13
@doc:
https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent-kafka-confluent-s-python-client-for-apache-kafka
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
"""
import logging

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from writers.base import BaseWriter
from utils.common import obj2bytes
from config.sys_config import (
    BROKER_VERSION,
    COMPRESSION_TYPE,
    BOOTSTRAP_SERVERS
)

logger = logging.getLogger(__name__)


class KafkaWriter(BaseWriter):
    @staticmethod
    def create_topics(topics, num_partitions=1, replication_factor=1,
                      bootstrap_servers=BOOTSTRAP_SERVERS):
        if not isinstance(topics, (list, tuple)):
            topics = [topics]

        admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        topic_futures = admin.create_topics(
            [NewTopic(topic, num_partitions, replication_factor)
                for topic in topics]
        )

        results = {}
        for topic, future in topic_futures.items():
            try:
                future.result()
            except (KafkaException, KafkaError) as e:
                logger.info(f'Create topic {topic} unsuccessfully: {e}')
                results.update(topic=False)
            else:
                logger.info(f'Create topic {topic} successfully')
                results.update(topic=True)
        return results

    @staticmethod
    def delete_topics(topics, bootstrap_servers=BOOTSTRAP_SERVERS):
        if not isinstance(topics, (list, tuple)):
            topics = [topics]

        admin = AdminClient({'bootstrap.servers': bootstrap_servers})
        topic_futures = admin.delete_topics(topics)

        results = {}
        for topic, future in topic_futures.items():
            try:
                future.result()
            except (KafkaException, KafkaError) as e:
                logger.info(f'Delete topic {topic} unsuccessfully: {e}')
                results.update(topic=False)
            else:
                logger.info(f'Delete topic {topic} successfully')
                results.update(topic=True)
        return results

    def __init__(self, topic=None, bootstrap_servers=BOOTSTRAP_SERVERS,
                 *args, **kwargs):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = self.get_producer()

    def on_deliver(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}, {msg}')
            if err.code() == KafkaError._MSG_TIMED_OUT:
                logger.error(f'{msg.topic()}, {msg}')
                self.producer = self.get_producer()
                self.producer.poll(0)
                self.producer.produce(msg.topic(), msg.value())
            else:
                raise KafkaError(f'Message delivery failed: {err}, {msg}')
        # else:
        #     logger.info(f'{msg.topic()} {msg.partition()} {msg.offset()}')

    def get_producer(self):
        return Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'broker.version.fallback': BROKER_VERSION,
            'compression.type': COMPRESSION_TYPE,
            'on_delivery': self.on_deliver
        })

    def write(self, obj):
        self.producer.poll(0)
        self.producer.produce(self.topic or obj.get('topic'), obj2bytes(obj))

    def commit(self):
        self.producer.flush(5)

    def stop(self):
        super().stop()
        self.producer = None
        logger.info(f'Kafka writer stopped')


if __name__ == '__main__':
    import json
    import time
    from utils.log import configure_logging

    configure_logging()

    ms = [
        {
            '_id': {
                '_data': '825D01F8F20000000129295A1004CA2EF16C952B4DFDA0B8A911DD5E4CD946645F696400645D01F8EEC665DA3DC9D421850004'
            },
            'operationType': 'insert',
            'clusterTime': (1560410354, 1),
            'fullDocument': {
                '_id': ('5d01f8eec665da3dc9d42185'),
                'lm_number': '2018122614102739934561',
                'app_name': 'xyf',
                'event_id': '1002107',
                'device_id': '20190613f538e1088',
                'user_id': '6065750',
                'session_id': '3783747965704725f0d42633a247f180',
                'source_type': 'client',
                'position': '2',
                'event_time': '1560410269041',
                'is_login': '1',
                'mobile': '13186668169',
                'token': '3783747965704725f0d42633a247f180',
                'os': 'android',
                'app': 'xyf',
                'channel': 'autoupdate',
                'version_code': '30400',
                'utm_source': '',
                'created_time': '2019-06-13 15:17:54'
            },
            'ns': {
                'db': 'reporting',
                'coll': 'bizEventReport2019'
            },
            'documentKey': {
                '_id': ('5d01f8eec665da3dc9d42185')
            }
        },
        {
            '_id': {
                '_data': '825D01FA7B0000000129295A1004CA2EF16C952B4DFDA0B8A911DD5E4CD946645F696400645D01FA7745C4B16BB4CFB1C50004'
            },
            'operationType': 'insert',
            'clusterTime': (1560410747, 1),
            'fullDocument': {
                '_id': ('5d01fa7745c4b16bb4cfb1c5'),
                'event_id': '1002466',
                'mobile': '',
                'type': '8',
                'event_time': '1560410274784',
                'token': 'eab967b7a66cb969333163a1ae183bad',
                'device_id': '',
                'source_type': 'wap',
                'os': 'ios',
                'app': 'xyf',
                'channel': '',
                'version_code': '',
                'utm_source': 'QD-CPS-XYF-YJ01',
                'created_time': '2019-06-13 15:17:54'
            },
            'ns': {
                'db': 'reporting',
                'coll': 'bizEventReport2019'
            },
            'documentKey': {
                '_id': ('5d01fa7745c4b16bb4cfb1c5')
            }
        }
    ]

    tc = 'reporting-bizEventReport2019'

    # KafkaWriter.delete_topics(tc)
    # KafkaWriter.create_topics(tc)

    w = KafkaWriter(tc)
    for i in range(20):
        for m in ms:
            w.write(json.dumps(m))
        time.sleep(i)
    w.stop()
