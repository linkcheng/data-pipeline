#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: db_base
@date: 2019-07-10 
"""
import logging
from bson import json_util

from readers.base import BaseReader
from utils.cache import Cache
from utils.str_utils import b2s
from config.sys_config import REDIS_CONFIG


logger = logging.getLogger(__name__)


class DBReader(BaseReader):
    suffix_rt = 'rt'
    suffix_ts = 'ts'

    @staticmethod
    def decode(args):
        raise NotImplementedError()

    @staticmethod
    def decode_ts(args):
        k, v = args
        return b2s(k), json_util.loads(v)

    @staticmethod
    def save(cache, value):
        if cache and value:
            logger.info(f'Save {cache.__repr__()}')
            cache.setm(value)

    def __init__(self, tables, cache_key='', is_bootstrap=True, is_resume=True):
        """从 DB 读取数据
        :param tables: list, 要读取的库表
        :param cache_key: 缓存key
        :param is_bootstrap: 是否全量查询
        :param is_resume: 是与否启用断点续传
        """
        self.tables = tables
        self.is_bootstrap = is_bootstrap
        self.is_resume = is_resume

        self._cache_key = cache_key
        # resume_token 用于记录实时增量日志读取的位置
        self._cache_resume_token = None
        self.resume_token = {}
        self._rt = {}

        # 用于记录每张表读取到的时间
        self._cache_timestamps = None
        self.timestamps = {}
        self._ts = {}

        if self.is_resume:
            key_rt = f'{cache_key}:{self.suffix_rt}'
            # 从外部缓存读取
            self._cache_resume_token = Cache(key_rt, **REDIS_CONFIG)
            self.resume_token = self._rt = self.get_resume_token()

            key_ts = f'{cache_key}:{self.suffix_ts}'
            # 从外部缓存读取
            self._cache_timestamps = Cache(key_ts, **REDIS_CONFIG)
            self.timestamps = self._ts = self.get_timestamps()

        # 新增表
        self.new_tables = set(self.tables) - set(self.timestamps.keys())
        # 保留表
        self.old_tables = set(self.tables) & set(self.timestamps.keys())
        # 需要跟踪增量表
        self.inc_tables = self.new_tables | self.old_tables

    def get_resume_token(self):
        resume_token = self._cache_resume_token.get_all() if self._cache_resume_token else {}
        return dict(map(self.decode, resume_token.items()))

    def save_resume_token(self):
        self.save(self._cache_resume_token, self.resume_token)

    def get_timestamps(self):
        timestamps = self._cache_timestamps.get_all() if self._cache_timestamps else {}
        return dict(map(self.decode_ts, timestamps.items()))

    def save_timestamps(self):
        timestamps = {k: json_util.dumps(v) for k, v in self.timestamps.items()}
        self.save(self._cache_timestamps, timestamps)

    def read(self):
        # 如果 self.keys 有新增数据库，也就是与 self.timestamps 中有不同
        # 则启动全量拉取
        if self.new_tables and self.is_bootstrap:
            yield from self.bootstrap()
            if self.is_resume:
                self.push()

        yield from self.watch()

    def bootstrap(self, *args, **kwargs):
        """读取历史数据"""
        raise NotImplementedError()

    def watch(self, *args, **kwargs):
        """读取增量数据"""
        raise NotImplementedError()

    def commit(self, *args, **kwargs):
        self.resume_token.update(self._rt)
        self.timestamps.update(self._ts)

    def push(self):
        self.save_resume_token()
        self.save_timestamps()

    def disconnect(self, *args, **kwargs):
        raise NotImplementedError()

    def stop(self):
        if self.is_resume:
            self.push()
            logger.info(f'Reader closed')


if __name__ == '__main__':
    pass
