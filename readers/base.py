#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base 
@date: 2019-06-03 
"""


class BaseReader:
    """base reader"""
    def read(self, *args, **kwargs):
        raise NotImplementedError()

    def commit(self, *args, **kwargs):
        raise NotImplementedError()

    def disconnect(self, *args, **kwargs):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


if __name__ == '__main__':
    pass
