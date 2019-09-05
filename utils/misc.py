#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: misc 
@date: 2019-07-17 
"""
import sys
import threading
from pymysqlreplication.gtid import GtidSet


def import_string(import_name):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    :param import_name: the dotted name for the object to import.

    :return: imported object
    """
    # force the import name to automatically convert to strings
    # __import__ is not able to handle unicode strings in the fromlist
    # if the module is a package
    import_name = str(import_name).replace(":", ".")

    try:
        __import__(import_name)
    except ImportError:
        if "." not in import_name:
            raise
    else:
        return sys.modules[import_name]

    module_name, obj_name = import_name.rsplit(".", 1)
    module = __import__(module_name, globals(), locals(), [obj_name])
    try:
        return getattr(module, obj_name)
    except AttributeError as e:
        raise ImportError(e)


class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__flag = threading.Event()  # 用于暂停线程的标识
        self.__flag.set()  # 设置为True
        self.__running = threading.Event()  # 用于停止线程的标识
        self.__running.set()  # 将running设置为True

    def pause(self):
        self.__flag.clear()  # 设置为False, 让线程阻塞

    def resume(self):
        self.__flag.set()  # 设置为True, 让线程停止阻塞

    def stop(self):
        self.__flag.set()  # 将线程从暂停状态恢复, 如何已经暂停的话
        self.__running.clear()  # 设置为False


class GtidDict:
    def __init__(self, gtid_set):
        self.gtid_set = GtidSet(gtid_set)
        self.gtid_dict = {gtid.sid: gtid for gtid in self.gtid_set.gtids}

    def is_equal(self, others):
        for other in others.gtid_dict.values():
            gtid = self.gtid_dict.get(other.sid)
            if not gtid:
                return False
            if gtid.sid != other.sid:
                return False
            if gtid.intervals != other.intervals:
                return False
        else:
            return True


if __name__ == '__main__':
    gtid_str1 = '034a41ae-d4cf-11e7-83e1-90e2bac72b7c:1-2739610290,' \
                '06f9db1d-b261-11e6-9e09-6c92bf13b922:1-26665078,' \
                '1b3e0b94-1ae4-11e7-878b-7cd30ac20c2c:1-401733433,' \
                'dc3df1e3-891a-11e8-9b90-506b4b2316a8:1-46,' \
                'e052efcc-2067-11e5-a0d6-8038bc0bae91:1-7107065,' \
                'e5ffd3d0-2067-11e5-a0d6-8038bc0bb0c9:1-1618721'

    gtid_str2 = '034a41ae-d4cf-11e7-83e1-90e2bac72b7c:1-2739618345,' \
                '06f9db1d-b261-11e6-9e09-6c92bf13b922:1-26665078,' \
                '1b3e0b94-1ae4-11e7-878b-7cd30ac20c2c:1-401733433,' \
                'dc3df1e3-891a-11e8-9b90-506b4b2316a8:1-46,' \
                'e052efcc-2067-11e5-a0d6-8038bc0bae91:1-7107065,' \
                'e5ffd3d0-2067-11e5-a0d6-8038bc0bb0c9:1-1618721'

    gtid_dict1 = GtidDict(gtid_str1)
    gtid_dict2 = GtidDict(gtid_str2)

    print(gtid_dict1.is_equal(gtid_dict2))
    print(gtid_dict1.is_equal(GtidDict(gtid_str1)))
