#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: local_executor 
@date: 2019-07-18 
"""
from executors.base_executor import BaseExecutor
from executors.common import Worker


class LocalExecutor(BaseExecutor):
    """本地进程"""

    def run(self):
        worker = Worker(
            self.reader_name, self.reader_args, self.reader_kwargs,
            self.writer_name, self.writer_args, self.writer_kwargs,
            self.operator_name
        )
        worker.run()
