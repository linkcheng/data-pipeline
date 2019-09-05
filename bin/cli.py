#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: cli 
@date: 2019-07-17
export PYTHONPATH={pwd}

python bin/cli.py \
    --tables='statistics.StarUser1' \
    --config='config.db_config.MYSQL_R_CONFIG'

python bin/cli.py \
    --tables='config.db_config.MYSQL_R_STATISTICS_TABLES' \
    --config='config.db_config.MYSQL_R_CONFIG'

python bin/cli.py \
    --source=Kafka \
    --sink=MySQL \
    --topics='statistics-StarUser1,statistics-StarUser2' \
    --group_id=group-1 \
    --client_id=default \
    --config='config.db_config.CS_WR_CONFIG'

python bin/cli.py \
    --tables='statistics.StarUser1' \
    --config='config.db_config.MYSQL_R_CONFIG' \
    --executor='docker' \
    --repo='base_images/data_pipeline_executor' \
    --tag='20190827' \
    --name='executor1'

"""
import click
from werkzeug.utils import import_string

from config.sys_config import SOURCES, SINKS, MQS, EXECUTORS
from utils.log import configure_logging


@click.command()
@click.option('--source', required=True, default='MySQL',
              type=click.Choice(SOURCES),
              help='source 或者 sink 必须有一个为 MQS 类型')
@click.option('--sink', required=True, default='Kafka',
              type=click.Choice(SINKS),
              help='source 或者 sink 必须有一个为 MQS 类型')
@click.option('--topics', default=None,
              help='要写入/读取的消息队列主题，如果不传入，写入时默认使用`tables`，'
                   '读取时不能为空必填')
@click.option('--group_id', default='group-1',
              help='从消息队列读取数据需要的消息组，默认值`group-1`')
@click.option('--client_id', default='default',
              help='从消息队列读取数据客户端ID，默认值`default`')
@click.option('--tables', default=None,
              help='要读取/写入的表(table)或者是集合(collection)，使用配置文件的配置项，'
                   '格式如：config.db_config.MYSQL_R_STATISTICS_TABLES；'
                   '或者直接传入带有 schema 的表名，多个以逗号`,`分隔，'
                   '格式如 schema1.table1,schema2.table2 格式的字符串')
@click.option('--config', required=True,
              help='数据库的配置信息，值为配置文件的配置项，格式如：config.db_config.CONFIG')
@click.option('--executor', default='local', type=click.Choice(EXECUTORS),
              help='启动方式，默认为 local')
@click.option('--name', default='default',
              help='当 executor=celery 时，用于配置队列名称；'
                   '当 executor=docker 时，用于配置容器名称；')
@click.option('--repo', help='当 executor=docker 时，为容器镜像仓库名称；')
@click.option('--tag', help='当 executor=docker 时，为容器镜像tag名称；')
def main(source, sink,
         topics, group_id, client_id,
         tables, config,
         executor, name,
         repo, tag):
    if source not in MQS and sink not in MQS:
        click.echo('source 或者 sink 必须有一个为 MQS 类型')
        return

    configure_logging()

    try:
        tables = import_string(tables)
    except ImportError:
        print(f'can not import values from tables argument, then use the value')

    config_kwargs = import_string(config)

    if executor == 'local':
        from executors.local_executor import LocalExecutor

        executor = LocalExecutor(source, sink, tables, topics, group_id,
                                 client_id, config_kwargs)
    elif executor == 'celery':
        from executors.celery_executor import CeleryExecutor

        executor = CeleryExecutor(source, sink, tables, topics, group_id,
                                  client_id,  config_kwargs, name)
    elif executor == 'docker':
        from executors.docker_executor import DockerExecutor

        cmd = build_cmd(
            source=source, sink=sink,
            topics=topics, group_id=group_id, client_id=client_id,
            tables=tables, config=config
        )
        print(f'docker container cmd={cmd}')
        executor = DockerExecutor(repo, tag, name, cmd)

    executor.run()


def build_cmd(**kwargs):
    args = [f'--{key}={value}' for key, value in kwargs.items() if value]
    return ' '.join(args)


if __name__ == '__main__':
    main()
