#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base_producer 
@date: 2019-07-06 
"""
import logging

from operators.base import Base
from operators.common import CommitPoint, BreakPoint

logger = logging.getLogger(__name__)


class BaseProducer(Base):
    """\
    每接受一条数据添加到缓存 buffer
    每次处理完一条都转给 writer，通常为消息队列
    """
    def route(self, obj):
        """用来区分是 bootstrap(全量), increment(增量), 包括: ddl, dml"""
        # logger.info(obj)
        self.buffer.put(obj)

    def delegate(self, obj):
        """针对不同的操作类型执行不同的操作"""
        if isinstance(obj, (CommitPoint, BreakPoint)):
            self.commit()
            return
        self.writer.write(obj)

    def commit(self):
        logger.info('Start Committing')
        self.writer.commit()
        self.reader.commit()


if __name__ == '__main__':
    pass
