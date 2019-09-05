#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link
@contact: zhenglong1992@126.com
@module: cache
@date: 2019-06-03
"""
import redis


class Cache(object):
    def __init__(self, name, host='localhost', port=6379, password=None, db=5):
        """
        host="localhost", port=6379, db=0
        :param host:
        :param port:
        :param db:
        :param name:
        """
        self._name = name
        self._cache = redis.StrictRedis(host=host, port=port, password=password, db=db)

    def __repr__(self):
        return self._name

    def get(self, key):
        """
        获取缓存
        :param key:
        :return: value
        :example: value = get('key')
        """

        return self._cache.hget(self._name, key)

    def set(self, key, value):
        """
        设置缓存
        :param key:
        :param value:
        :return:
        :example: set('key', 'value')
        """
        return self._cache.hset(self._name, key, value)

    def getm(self, keys):
        """
        获取缓存
        :param keys: list or tuple
        :return: list
        :example: values = getm(('key1', 'key2'))
        """
        try:
            iter(keys)
            if isinstance(keys, (str, bytes)):
                keys = [keys]
        except TypeError:
            keys = [keys]

        return self._cache.hmget(self._name, keys)

    def get_all(self):
        """
        获取所有缓存
        :return: list
        """
        return self._cache.hgetall(self._name)

    def setm(self, mapping):
        """
        设置缓存
        :param mapping: {key1: value1, key2: value2}
        :return:
        :example: setm({'key1': 'value1', 'key2': 'value2'})
        """
        return self._cache.hmset(self._name, mapping)

    def delm(self, keys):
        """
        删除缓存
        :param keys: list or tuple
        :return: list
        :example: delm(('key1', 'key2'))
        """
        try:
            iter(keys)
            if isinstance(keys, (str, bytes)):
                keys = [keys]
        except TypeError:
            keys = [keys]
        return self._cache.hdel(self._name, keys)

    def remove(self):
        """
        清除缓存
        :return:
        """
        self._cache.delete(self._name)


def make_key(*args, **kwargs):
    """
    生成 key
    :param args:
    :return:
    """
    key = list()
    for arg in args:
        if isinstance(arg, (str, int)):
            key.append(arg)

    return tuple(key)


def cache(mem):
    """
    缓存装饰器
    :param mem: Cache 实例
    :return:
    """
    def _cache(fn):
        def __cache(*args, **kwargs):
            key = make_key(*args)
            value = mem.get(key)

            if not value:
                value = fn(*args, **kwargs)
                mem.set(key, value)
            return value
        return __cache
    return _cache
