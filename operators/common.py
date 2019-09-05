#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link
@contact: zhenglong1992@126.com
@module: common
@date: 2019-06-16
"""
from queue import Queue
from munch import Munch


class Buffer(Queue):
    """基于 Queue 的 put 与 get"""


class CommitPoint:
    """用于判断缓存需要提交的节点"""


class DateNode(Munch):
    """\
    数据节点，直接把 dict 转化为对象，
    仅是把字典的第一层 key 转化为对象属性，多层级 key 依然保留原来的数据结构
    """


class BreakPoint:
    """用于判断缓存队列需要结束终止的节点"""
