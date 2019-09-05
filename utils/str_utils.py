#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: number_utils 
@date: 2019-07-16 
"""

import logging

logger = logging.getLogger(__name__)


def s2i(s, default=None):
    """str 转 int"""
    try:
        i = int(s)
    except ValueError as e:
        logger.error(f's={s}, e={str(e)}')
        i = default
    return i


def s2f(s, default=None):
    """字符串转小数"""
    try:
        f = float(s)
    except Exception as e:
        logger.error(f's={s}, e={str(e)}')
        f = default
    return f


def s2num(val, default=None):
    """低效将字符串转数字，但兼容性好"""
    if val is None:
        return default

    if val.isdigit() or val[1:].isdigit():
        ret = int(val)
    elif '.' in val:
        try:
            ret = float(val)
        except Exception as e:
            logger.error(str(e))
            ret = default
    else:
        ret = default
    return ret


def b2s(val):
    """byte -> str"""
    if isinstance(val, bytes):
        val = val.decode('utf-8')
    else:
        val = str(val)

    return val


def s2b(val):
    """str -> byte"""
    if isinstance(val, str):
        ret = val.encode('utf-8')
    else:
        try:
            ret = bytes(val)
        except Exception as e:
            logger.error(f'val={val}, e={str(e)}')
            ret = None
    return ret


def ensure_unicode(val):
    return b2s(val)


if __name__ == '__main__':
    print(s2num('是'))
    print(s2num('否'))
    print(s2num(''))
    print(s2num('1'))
    print(s2num('1.0'))
    print(s2num('-1'))
    print(s2num('-1.0'))
    print(s2f(''))
    print(s2f(1))
    print(s2f(1.0))
    print(s2f('1'))
    print(s2f('-1'))
    print(s2f('1.2'))
    print(s2f('-1.2'))
    print(s2f('1.a'))
    print(s2i(1))
    print(s2i(''))
    print(s2i('2'))
    print(s2i('2.0'))
    print(s2i('-2'))
