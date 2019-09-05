#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: common 
@date: 2019-07-18 
"""
import sys
import signal
import logging
import inspect
from werkzeug.utils import find_modules
from werkzeug.utils import import_string

logger = logging.getLogger(__name__)


class Router:
    PATH = {
        'reader': 'readers',
        'writer': 'writers',
        'operator': 'operators',
    }

    FINDER = {
        'readers': lambda obj: (
            inspect.isclass(obj)
            and hasattr(obj, 'read')
            and hasattr(obj, 'commit')
            and hasattr(obj, 'stop')
            and obj.__name__ not in ('BaseReader', 'DBReader', 'MQReader')
        ),
        'writers': lambda obj: (
            inspect.isclass(obj)
            and hasattr(obj, 'write')
            and hasattr(obj, 'commit')
            and hasattr(obj, 'stop')
            and obj.__name__ not in ('BaseWriter', )
        ),
        'operators': lambda obj: (
            inspect.isclass(obj)
            and hasattr(obj, 'route')
            and hasattr(obj, 'delegate')
            and hasattr(obj, 'read')
            and hasattr(obj, 'write')
            and hasattr(obj, 'run')
            and hasattr(obj, 'stop')
            and obj.__name__ not in ('Base', 'BaseProducer', 'BaseConsumer')
        ),
    }

    @classmethod
    def get_class(cls, unit_type, class_name):
        path = cls.PATH.get(unit_type)
        finder = cls.FINDER.get(path)

        for module_name in find_modules(path, recursive=True):
            module = import_string(module_name)
            members = inspect.getmembers(module, finder)
            for member_name, class_ in members:
                if class_name == member_name:
                    return class_


class Worker:
    def __init__(self, reader_name, reader_args, reader_kwargs,
                 writer_name, writer_args, writer_kwargs,
                 operator_name):
        self.reader_name = reader_name
        self.writer_name = writer_name
        self.operator_name = operator_name

        self.reader_args = reader_args
        self.reader_kwargs = reader_kwargs
        self.writer_args = writer_args
        self.writer_kwargs = writer_kwargs

        self.reader_cls = Router.get_class('reader', self.reader_name)
        self.writer_cls = Router.get_class('writer', self.writer_name)
        self.operator_cls = Router.get_class('operator', self.operator_name)

        self.reader = self.reader_cls(*self.reader_args, **self.reader_kwargs)
        self.writer = self.writer_cls(*self.writer_args, **self.writer_kwargs)
        self.operator = self.operator_cls(self.reader, self.writer)

    def __del__(self):
        self.stop()

    def _term_handler(self, signal_num, frame):
        """系统信号量处理"""
        logger.info(f'Get signal={signal_num}, try to exit')
        self.stop()
        sys.exit(1)

    def run(self):
        signal.signal(signal.SIGTERM, self._term_handler)
        signal.signal(signal.SIGINT, self._term_handler)
        self.operator.run()

    def stop(self):
        try:
            self.operator.stop()
        except AttributeError:
            pass
