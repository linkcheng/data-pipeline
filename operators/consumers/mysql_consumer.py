#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mysql_consumer 
@date: 2019-07-14 
"""
import logging
import base64

from .base_consumer import BaseConsumer
from operators.common import CommitPoint, DateNode, BreakPoint
from utils.common import bytes2obj

logger = logging.getLogger(__name__)


class MySQLConsumer(BaseConsumer):
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
    )

    def __init__(self, reader, writer, keys=None, binaries=None):
        """\
        处理 reader 产生的数据，写入目标库
        :param reader: 获取数据的对象, BaseReader 子类实例
        :param writer: 写入数据的对象, BaseWriter 子类实例
        :param keys: 用户读取 reader 产生的数据的key, list
        :param binaries: reader 数据源的数据中类型为 binary 的列名称, list
        """
        super().__init__(reader, writer)
        # 记录上次处理的事务 id
        # self.latest_xid = 0
        # 用于 sql 构造器中从数据源抽取数据
        self.keys = keys
        self.binaries = binaries or []

    def delegate(self, obj):
        if isinstance(obj, (CommitPoint, BreakPoint)):
            self.commit()
            return

        if obj.type in ('bootstrap-insert', 'insert'):
            self.writer.insert(obj.data, self.keys, obj.database, obj.table)
        elif obj.type == 'update':
            self.writer.update(obj.data, obj.old, self.keys,
                               obj.database, obj.table)
        elif obj.type == 'delete':
            self.writer.delete(obj.data, self.keys, obj.database, obj.table)

        self.last = obj

    def route(self, message):
        """对 message 进行路由处理"""
        if isinstance(message, (CommitPoint, BreakPoint)):
            self.commit()
            return

        token = self.reader.get_token(message)
        value = bytes2obj(self.reader.get_value(message))
        value['token'] = token
        # 对特殊类型字段值做处理
        value = self.map(value)

        action_type = value.get('type')
        if action_type in self.bootstrap_mode:
            self._bootstrap(action_type, value)
        elif action_type in self.dml_mode:
            self._dml(action_type, value)

    def map(self, value):
        data = value.get('data')
        if not data:
            return value

        # binary 类型字段要做 base64.decode
        for col in self.binaries:
            if data[col]:
                data[col] = base64.b64decode(data[col])

        # todo: 5.7+ json 类型数据转换
        return value

    def _bootstrap(self, action_type, value):
        """bootstrap 模式处理
        :param action_type: bootstrap 的具体操作类型
            'bootstrap-start',
            'bootstrap-insert',
            'bootstrap-complete',
        :param value: 需要处理的数据
        :return: 是否需要熟悉缓存区域
        """
        if action_type == 'bootstrap-insert':
            self.buffer.put(DateNode(value))
        # bootstrap 模式结束或者超过缓存最大限度 就应该加入刷新点
        if (action_type == 'bootstrap-complete'
                or self.buffer.qsize() + 1 >= self.max_buf_size):
            self.buffer.put(CommitPoint())

    def _dml(self, action_type, value):
        """dml 模式处理
        :param action_type: 具体操作类型
        :param value: 需要处理的数据
        :return:
        """
        # 同一个事务 xid 相同，
        # 如果事务提交则：`"commit":true`
        # 否则有 `"offset":n`，n 表示当前事务的顺序，从 0 开始递增
        # 当事务涉及多张表的写操作的时候，可能事务提交在其他的表中，但是新来的事务 xid 会变化
        commit = value.get('commit')
        self.buffer.put(DateNode(value))
        if commit or self.buffer.qsize() + 1 >= self.max_buf_size:
            self.buffer.put(CommitPoint())

    def _ddl(self, action_type, value):
        """ddl 模式处理
        :param action_type: 具体操作类型
        :param value: 需要处理的数据
        :return:
        """
