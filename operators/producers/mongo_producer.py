#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mongo_producer 
@date: 2019-06-16 
"""

from .base_producer import BaseProducer


class MongoDBProducer(BaseProducer):
    """从 MongoDB 获取数据写入 writer"""


if __name__ == '__main__':
    pass
