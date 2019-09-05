#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base_executor 
@date: 2019-07-18 
"""
import logging

from config.sys_config import MQS

logger = logging.getLogger(__name__)


class BaseExecutor:

    def __init__(self, source, sink, tables, topics, group_id, client_id,
                 config: dict, *args, **kwargs):
        """当 sink in MQS 时，代表写入消息队列，意味着 source 应该为 DB,
        那么 tables 跟 config 就是必配选项，topics 可以为空表示使用 tables 值，即每张表对应一个主题；
        如果指定 topics 为某个字符串值，那么所有表的数据写入同一个 topic。

        相反，sink not in MQS 时，也就是从消息队列读取数据，写入 DB，此时可以订阅多个 topics，
        然后从 topics 中读取消息中的数据库信息，写入对应的表中，此时 tables 无效；
        如果从 topics 中读取消息中没有获取到数据库信息，那么会使用 tables。
        """
        self.source = source
        self.sink = sink
        self.tables = tables
        self.topics = topics
        self.group_id = group_id
        self.client_id = client_id
        self.config = config

        self.reader_name = source + 'Reader'
        self.writer_name = sink + 'Writer'
        logger.info(f'reader_name={self.reader_name},'
                    f'writer_name={self.writer_name}')

        if self.sink in MQS:
            self.operator_name = source + 'Producer'
            logger.info(f'operator_name={self.operator_name}')

            if not isinstance(self.tables, (list, tuple, set)):
                logger.info('tables must be list/tuple/set, when sink is a MQ')
                self.tables = self.tables.split(',')

            self.reader_args = (self.tables, )
            self.reader_kwargs = self.config
            logger.info(f'Reader tables: {self.tables}, config: {self.config}')

            self.writer_args = (self.topics, )
            self.writer_kwargs = {}
            logger.info(f'Writer topics: {self.topics}')
        else:
            self.operator_name = sink + 'Consumer'
            logger.info(f'operator_name={self.operator_name}')

            if not isinstance(self.topics, (list, tuple)):
                logger.info('topics must be list/tuple/set, when sink is not MQ')
                self.topics = self.topics.split(',')

            self.reader_args = (self.topics, self.group_id, self.client_id)
            self.reader_kwargs = {}
            logger.info(f'Reader topics: {self.topics}, '
                        f'group_id: {self.group_id}, '
                        f'client_id: {self.client_id}')

            if self.tables and not isinstance(self.tables, str):
                raise ValueError('tables must be a string like schema.table, '
                                 'when sink is not a MQ')

            self.writer_args = self.tables.split('.') if self.tables else (None, None)
            self.writer_kwargs = self.config
            logger.info(f'Writer table: {self.tables}, config: {self.config}')

    def run(self):
        raise NotImplementedError()


if __name__ == '__main__':
    pass
