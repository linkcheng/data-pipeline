#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base 
@date: 2019-06-04 
"""
from config.sys_config import MAX_LEN


class BaseWriter:
    maxlen = MAX_LEN

    """base writer"""
    def write(self, *args, **kwargs):
        """使用两阶段提交方式，第一阶段准备阶段，通常保存到缓存队列"""
        raise NotImplementedError()

    def commit(self, *args, **kwargs):
        """使用两阶段提交方式，第二阶段提交阶段，提交缓存队列中的数据"""
        raise NotImplementedError()

    def stop(self):
        self.commit()


if __name__ == '__main__':
    pass
