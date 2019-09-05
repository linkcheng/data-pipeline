#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mq_base 
@date: 2019-07-14 
"""

import logging

from readers.base import BaseReader
from utils.cache import Cache
from config.sys_config import REDIS_CONFIG

logger = logging.getLogger(__name__)


class MQReader(BaseReader):
    suffix = 'rt'

    @staticmethod
    def decode(args):
        raise NotImplementedError()

    @staticmethod
    def get_value(msg):
        raise NotImplementedError()

    @staticmethod
    def get_token(msg):
        raise NotImplementedError()

    def __init__(self, cache_key='', is_bootstrap=True, is_resume=True):
        """从 MQ 读取数据
        :param cache_key: 缓存key
        :param is_bootstrap: 是否全量读取
        :param is_resume: 是否断点续传
        """
        self.is_bootstrap = is_bootstrap
        self.is_resume = is_resume

        self._cache_key = cache_key
        self._cache_resume_token = None
        self.resume_token = {}

        if self.is_resume:
            key = f'{cache_key}:{self.suffix}'
            # 从外部缓存读取
            self._cache_resume_token = Cache(key, **REDIS_CONFIG)
            self.resume_token = self.get_resume_token()

    def get_resume_token(self):
        resume_token = self._cache_resume_token.get_all() if self._cache_resume_token else {}
        return dict(map(self.decode, resume_token.items()))

    def save_resume_token(self):
        if self._cache_resume_token and self.resume_token:
            logger.info('Save resume_token')
            self._cache_resume_token.setm(self.resume_token)

    def read(self):
        raise NotImplementedError()

    def commit(self, token):
        if self.is_resume:
            self.resume_token.update(token)

    def disconnect(self, *args, **kwargs):
        raise NotImplementedError()

    def stop(self):
        self.save_resume_token()
        logger.info(f'Reader closed')
