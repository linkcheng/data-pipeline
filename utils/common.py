#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link
@contact: zhenglong1992@126.com
@module: common
@date: 2019-06-03
"""
import time
import json
import logging
import base64
from datetime import datetime, date
from functools import wraps

from .date_utils import t2s, d2s
from .str_utils import b2s

logger = logging.getLogger(__name__)


def fn_timer(fn):
    """计算 fn 的运算时间"""
    @wraps(fn)
    def function_timer(*args, **kwargs):
        start = time.time()
        result = fn(*args, **kwargs)
        end = time.time()
        logger.info(f'{fn.func_name} total running time {end - start} seconds')
        return result

    return function_timer


def obj2bytes(obj):
    if isinstance(obj, bytes):
        return obj
    return bytes(json.dumps(obj, cls=DateEncoder), encoding='utf-8')


def bytes2obj(bts: bytes):
    if not isinstance(bts, bytes):
        return bts
    return json.loads(bts)


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return t2s(obj)
        elif isinstance(obj, date):
            return d2s(obj)
        elif isinstance(obj, bytes):
            return b2s(base64.b64encode(obj))
        else:
            try:
                return json.JSONEncoder.default(self, obj)
            except TypeError:
                return str(obj)
