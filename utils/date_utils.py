#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: date_utils 
@date: 2019-07-16 
"""
import logging
from pytz import timezone
from datetime import datetime, date

str_fmt = '%Y-%m-%d %H:%M:%S'
str_fmt_d = '%Y-%m-%d'
str_fmt_dt = '%Y-%m-%d %H:%M:%S'

date_start = date(1970, 1, 1)
time_start = datetime(1970, 1, 1, 0, 0, 0)
date_start_str = '1970-01-01'
time_start_str = '1970-01-01 00:00:00'

cst_tz = timezone('Asia/Shanghai')
utc_tz = timezone('UTC')

logger = logging.getLogger(__name__)


def t2d(dt, default=date_start):
    """datetime 转 date"""
    if isinstance(dt, str):
        dt = s2t(dt)
    try:
        d = dt.date()
    except Exception as e:
        logger.error(f's={dt}, e={str(e)}')
        d = default
    return d


def t2s(dt, fmt=str_fmt):
    """datetime 转 str"""
    if isinstance(dt, str):
        return dt
    try:
        s = dt.strftime(fmt)
    except Exception as e:
        logger.error(f's={dt}, e={str(e)}')
        s = None
    return s


def d2s(d, fmt=str_fmt_dt):
    """date 转 str"""
    return t2s(d, fmt)


def s2d(s, fmt=str_fmt):
    """str 转 date"""
    dt = s2t(s, fmt)
    return dt.date() if dt else None


def s2t(s, fmt=str_fmt):
    """str 转 datetime"""
    if s is None:
        return None
    val, *end = s.split('.')
    try:
        dt = datetime.strptime(val, fmt) if val else None
    except Exception as e:
        logger.error(f's={val}, e={str(e)}')
        dt = None
    return dt


def set_tz(dt, tz=utc_tz):
    """设置时区"""
    if dt is None:
        return None
    return dt.replace(tzinfo=tz)


def to_utc(dt):
    """转换时区为 UTC"""
    if dt is None:
        return None
    return dt.astimezone(utc_tz)


def to_cst(dt):
    """设置为 CST"""
    if dt is None:
        return None
    return dt.astimezone(cst_tz)


if __name__ == '__main__':
    now = datetime.now()
    print(t2s(now))
    print(s2t('2018-01-01 10:00:00', '%Y-%m-%d %H:%M:%S'))
    print(s2d('2018-01-01 10:00:00', '%Y-%m-%d %H:%M:%S'))

