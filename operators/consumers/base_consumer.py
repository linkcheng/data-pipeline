#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base_consumer 
@date: 2019-06-29 
"""
import abc
import logging
from operators.base import Base

logger = logging.getLogger(__name__)


class BaseConsumer(Base, metaclass=abc.ABCMeta):
    """\
    每接受一条数据，经过路由处理，添加到缓存 buffer: dict，添加的数据结构为 {key: value}
    每次处理完一条都要根据上下文，通常是缓存 buffer 的长度以及是否在数据库事务当中，
    来判断出是否需要添加一个 CommitPoint，用来强制刷新 buffer，防止 OOM 以及保证事务性
    """

    def __init__(self, reader, writer):
        super().__init__(reader, writer)
        # 记录上一条处理的消息, 根据消息来源不同可能有不同的属性，
        # 但至少有 token 属性，并且值为字典类型，用与保存断点续传的表示
        self.last = None

    def commit(self):
        logger.info('Start Committing')
        self.writer.commit()
        logger.info('Writer Committed')
        if self.last:
            self.reader.commit(self.last.token)
            logger.info('Reader Committed')
