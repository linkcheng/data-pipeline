#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: celery_executor
@date: 2019-07-17
workers: celery worker -A executors.celery_executor -l info -Q Statistics
            --autoscale=30,3
监控: flower -A executors.celery_executor
        --broker="redis://localhost:6379/6"
        --address=0.0.0.0 --port=5555
        --persistent=True --db=./flower
启动: $ python bin/cli.py  --source=MySQL --sink=Kafka \
        --schema=statistics --table=StarUser2  \
        --config=config.db_config.STATISTICS_WR_CONFIG \
        --executor=celery  --queue=default

"""
from celery import Celery
from celery.utils.log import get_task_logger

from executors.base_executor import BaseExecutor
from executors.common import Worker
from config.sys_config import CELERY_NAME, CELERY_CONFIG

logger = get_task_logger(__name__)

app = Celery(
    CELERY_NAME,
    include=['executors.celery_executor'],
)

app.config_from_object(CELERY_CONFIG)


class CeleryExecutor(BaseExecutor):

    def __init__(self, source, sink, schema, table, topic, group_id, config,
                 queue, *args, **kwargs):
        super().__init__(source, sink, schema, table, topic, group_id, config)
        self.queue = queue

    def run(self):
        app.send_task(
            'executors.celery_executor.run',
            args=(
                self.reader_name, self.reader_args, self.reader_kwargs,
                self.writer_name, self.writer_args, self.writer_kwargs,
                self.operator_name
            ),
            queue=self.queue,
            ignore_result=True,
        )


@app.task
def run(reader_name, reader_args, reader_kwargs,
        writer_name, writer_args, writer_kwargs, operator_name):
    worker = Worker(reader_name, reader_args, reader_kwargs,
                    writer_name, writer_args, writer_kwargs, operator_name)
    worker.run()


if __name__ == '__main__':
    pass
