#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mongo_consumer 
@date: 2019-06-29 
"""
import logging

from .base_consumer import BaseConsumer
from operators.common import CommitPoint, DateNode, BreakPoint
from utils.common import bytes2obj

logger = logging.getLogger(__name__)


class MongoDBConsumer(BaseConsumer):
    """\
    每接受一条数据，经过路由处理，添加到缓存 buffer: dict，添加的数据结构为 {key: value}
    每次处理完一条都要根据上下文，通常是缓存 buffer 的长度以及是否在数据库事务当中，
    来判断出是否需要添加一个 flush 点，用来强制刷新 buffer，防止 OOM 以及保证 事务性
    """
    bootstrap_mode = (
        'bootstrap-start',
        'bootstrap-insert',
        'bootstrap-complete',
    )
    dml_mode = (
        'insert',
        'update',
        'delete',
        'replace',
    )

    def delegate(self, obj):
        if isinstance(obj, (CommitPoint, BreakPoint)):
            self.commit()
            return

        db_name = obj.ns.get('db')
        coll_name = obj.ns.get('coll')

        if obj.operationType in ('bootstrap-insert', 'insert',):
            self.writer.insert(obj.fullDocument, db_name, coll_name)
        if obj.operationType in ('update', 'replace',):
            self.writer.update(obj.fullDocument, obj.documentKey,
                               db_name, coll_name)
        elif obj.operationType == 'delete':
            self.writer.delete(obj.documentKey, db_name, coll_name)

        self.last = obj

    def route(self, message):
        """对 message 进行路由处理"""
        if isinstance(message, (CommitPoint, BreakPoint)):
            self.commit()
            return

        token = self.reader.get_token(message)
        value = bytes2obj(self.reader.get_value(message))
        value['token'] = token

        operation_type = value.get('operationType')
        if operation_type in self.bootstrap_mode:
            self._bootstrap(operation_type, value)
        else:
            self._dml(value)

    def _bootstrap(self, operation_type, value):
        """bootstrap 模式处理
        :param operation_type: bootstrap 的具体操作类型
            'bootstrap-start',
            'bootstrap-insert',
            'bootstrap-complete',
        :param value: 需要处理的数据
        :return: 是否需要熟悉缓存区域
        """
        if operation_type == 'bootstrap-insert':
            self.buffer.put(DateNode(value))

        # bootstrap 模式结束或者超过缓存最大限度 就应该加入刷新点
        if (operation_type == 'bootstrap-complete'
                or self.buffer.qsize() + 1 >= self.max_buf_size):
            self.buffer.put(CommitPoint())

    def _dml(self, value):
        """dml 模式处理"""
        self.buffer.put(DateNode(value))
        # 暂时没有事务处理
        self.buffer.put(CommitPoint())

    def _ddl(self, *args, **kwargs):
        """ddl 模式处理"""


if __name__ == '__main__':
    pass
