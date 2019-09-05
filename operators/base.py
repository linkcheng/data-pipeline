#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: base 
@date: 2019-06-03 
"""
import abc
import time
import logging
from threading import Thread

from operators.common import Buffer, BreakPoint
from config.sys_config import MAX_BUF_SIZE

logger = logging.getLogger(__name__)


class Base(metaclass=abc.ABCMeta):
    def __init__(self, reader, writer):
        # 缓冲队列最大值
        self.max_buf_size = MAX_BUF_SIZE
        # 消息会产生大量数据，需要一个缓冲队列
        self.buffer = Buffer(self.max_buf_size)

        # 数据获取者
        self.reader = reader
        # 数据输出地
        self.writer = writer

        # 初始化读数据子线程
        self.reader_thread = Thread(target=self.read, daemon=True)
        # 初始化写数据子线程
        self.writer_thread = Thread(target=self.write, daemon=True)

        self.is_running = False

    def read(self):
        """子线程负责获取数据"""
        for obj in self.reader.read():
            self.route(obj)
            if isinstance(obj, BreakPoint):
                break

    @abc.abstractmethod
    def route(self, obj):
        """针对不同的操作类型执行不同的操作"""
        pass

    def write(self):
        """子线程负责从缓冲队列 buffer 获取数据"""
        while True:
            try:
                obj = self.buffer.get()
            except KeyboardInterrupt as e:
                logger.error(e)
                break
            except Exception as e:
                logger.error(e)
                break
            else:
                self.delegate(obj)
                if isinstance(obj, BreakPoint):
                    break

    @abc.abstractmethod
    def delegate(self, obj):
        """针对不同的操作类型执行不同的操作"""
        pass

    def run(self):
        """主线程负责执行子线程生成的任务"""
        self.reader_thread.start()
        self.writer_thread.start()

        self.is_running = True
        self.reader_thread.join()
        self.writer_thread.join()

    def stop(self):
        if self.is_running:
            self.reader.disconnect()
            self.buffer.put(BreakPoint())

            if self.writer_thread.is_alive():
                while not self.buffer.empty():
                    logger.info(f'Buffer size is {self.buffer.qsize()}')
                    time.sleep(0.5)
                logger.info(f'Buffer is empty')

            self.writer.stop()
            self.reader.stop()
            self.is_running = False


if __name__ == '__main__':
    pass
