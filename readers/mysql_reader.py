#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mysql_reader 
@date: 2019-06-12
@note: 需要所有库的读权限
"""
import time
import logging
import json
from datetime import datetime

from bson import Timestamp
from werkzeug.utils import cached_property
from pymysql.cursors import SSDictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.gtid import GtidSet, Gtid
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from pymysqlreplication.event import (
    RotateEvent,
    GtidEvent
)

from connectors.mysql_connector import MySQLConnector
from readers.db_base import DBReader
from utils.common import DateEncoder
from utils.str_utils import b2s

logger = logging.getLogger(__name__)


class MySQLReader(DBReader):
    only_events = (
        DeleteRowsEvent,
        WriteRowsEvent,
        UpdateRowsEvent,
        RotateEvent,
        GtidEvent,
    )

    @staticmethod
    def decode(args):
        k, v = args
        key = b2s(k)
        return key, b2s(v) if key in ('log_file', 'auto_position') else int(v)

    def __init__(self, tables, client_id='default', *, host, port, user, password,
                 charset, database=None, autocommit=True, db=None,
                 cursorclass=SSDictCursor, is_bootstrap=True, is_resume=True):
        """从 MySQL 读取数据
        :param tables: list, 要读取的库表
        :param client_id: str, 用于区分不同的客户端
        :param user: MySQL 用户
        :param password: MySQL 密码
        :param host: MySQL 主机名
        :param port: MySQL 端口号
        :param charset: MySQL 字符集
        :param database: 要连接 MySQL 数据库
        :param autocommit: 是否开启自动提交
        :param db: 要连接数据库，作用同 database
        :param cursorclass: 连接数据库用的 cursor 类型
        :param is_bootstrap: 是否全量查询
        :param is_resume: 是否断点续传
        """
        cache_key = f'mysql:{client_id}'
        super().__init__(tables, cache_key, is_bootstrap, is_resume)
        self.conn = MySQLConnector(host=host, port=port, user=user,
                                   password=password, charset=charset,
                                   database=database, autocommit=autocommit,
                                   db=db, cursorclass=cursorclass)
        self.mysql_settings = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
        }
        self.client_id = client_id
        self.db_names = set()
        self.table_names = set()
        if isinstance(tables, (list, tuple, set)):
            for key in tables:
                db_name, table_name = key.split('.')
                self.db_names.add(db_name)
                self.table_names.add(table_name)

        self.stream = None
        self.server_id = int(time.time())
        self.auto_position = ''
        self.gtid_set = GtidSet(self.auto_position)
        # 是否所有表都对齐了
        self.has_aligned = False

        if self.is_resume:
            self.server_id = self.resume_token.get('server_id') or self.server_id
            self.set_auto_position(self.resume_token.get('auto_position') or self.auto_position)

    def set_auto_position(self, auto_position):
        self.auto_position = auto_position
        self.gtid_set = GtidSet(auto_position)

    def bootstrap(self):
        """读取历史数据"""
        tx_isolation = self.conn.get_tx_isolation()
        try:
            # 打开全局读锁
            self.conn.lock_tables()
            # 修改事务隔离级别为 REPEATABLE-READ
            self.conn.set_tx_isolation('REPEATABLE-READ')
            # 开启事务的一致性快照, 仅支持 innodb
            self.conn.start_transaction(with_snapshot=True)
            # 记录全局事务 ID
            auto_position = self.conn.get_global_gtid_executed()
        finally:
            self.conn.unlock_tables()

        logger.info(auto_position)
        self.set_auto_position(auto_position)

        for key in self.new_tables:
            schema, table = key.split('.')
            yield from self._bootstrap(schema, table)

        # 判断是否对齐：第一次属于对齐，其他都是非对齐
        self.has_aligned = not bool(self._rt)

        self._rt = self._rt or {
            'server_id': self.server_id,
            'resume_stream': int(False),
            'auto_position': self.auto_position,
        }
        logger.info(f'MySQL resume_token={self._rt}')

        # 关闭事务
        self.conn.commit()
        # 还原事务隔离级别
        self.conn.set_tx_isolation(tx_isolation)
        self.commit()

    def _bootstrap(self, db_name, table_name):
        """读取历史数据"""
        # 采样计数器
        counter = 1
        # 记录时间
        ts = Timestamp(datetime.utcnow(), 0)
        # 发送开始数据
        yield dict(type='bootstrap-start', database=db_name, table=table_name,
                   topic=f'{db_name}-{table_name}', ts=ts.time, data={})

        # 防止影响数据库缓存，禁止缓存查询结果
        sql = f'select sql_no_cache * from {db_name}.{table_name}'

        try:
            # 读取数据
            for msg in self.conn.read(sql, buffered=False):
                data = dict(type='bootstrap-insert', database=db_name,
                            table=table_name, topic=f'{db_name}-{table_name}',
                            ts=ts.time, data=msg)
                yield data
                # 添加采样数据到日志
                counter += 1
                if counter % 50_000 == 0:
                    logger.info(f"MySQL counter = {counter}, data={data}")
        except Exception:
            logger.error(f'MySQL error in {db_name}.{table_name}')
        finally:
            # 发送结束数据
            yield dict(type='bootstrap-complete', database=db_name,
                       table=table_name,
                       topic=f'{db_name}-{table_name}', ts=ts.time, data={})
            # 记录时间
            self._ts.update({
                f'{db_name}.{table_name}': ts
            })
            logger.info(f'MySQL timestamp={self._ts}')

    @cached_property
    def _log_file(self):
        stream_config = dict(
            connection_settings=self.mysql_settings,
            server_id=int(time.time()),
            only_events=[RotateEvent],
            only_schemas=self.db_names,
            only_tables=self.table_names,
            freeze_schema=True,
        )

        log_file = None
        stream = BinLogStreamReader(**stream_config)
        for binlog_event in stream:
            log_file = binlog_event.next_binlog
            break
        stream.close()
        return log_file

    def watch(self):
        """基于 binlog 实时解析数据
        第一次启动使用 resume_stream=False
        之后使用 resume_stream=True，保持 server_id 不变
        """
        stream_config = self.build_stream_config()
        logging.info(stream_config)
        self.stream = BinLogStreamReader(**stream_config)

        for binlog_event in self.stream:
            yield from self.parse_binlog(binlog_event)

    def build_stream_config(self):
        """构建 stream 的参数"""
        blocking = True
        freeze_schema = True
        log_file = None
        log_pos = None
        auto_position = None

        if self.is_resume:
            # 如果是第一次启动 resume_token = {},则 resume_stream 应该设置为 False
            resume_stream = bool(self._rt.get('resume_stream'))
            log_file = self._rt.get('log_file')
            log_pos = self._rt.get('log_pos')
            # 获取 gtid
            if not log_file or not log_pos:
                auto_position = self._rt.get('auto_position') or self.auto_position
        else:
            resume_stream = True
            auto_position = None

        return dict(
            connection_settings=self.mysql_settings,
            server_id=self.server_id,
            only_events=self.only_events,
            blocking=blocking,
            only_schemas=self.db_names,
            only_tables=self.table_names,
            log_file=log_file,
            log_pos=log_pos,
            auto_position=auto_position,
            resume_stream=resume_stream,
            # 暂不读取 DDL, 效率高
            freeze_schema=freeze_schema,
        )

    def parse_binlog(self, binlog_event):
        """解析 binlog"""
        if isinstance(binlog_event, GtidEvent):
            gtid = binlog_event.gtid
            if not self.has_aligned and not Gtid(gtid) in self.gtid_set:
                self.has_aligned = True
                logger.info(f'has_aligned gtid: {gtid}')
            return

        if isinstance(binlog_event, RotateEvent):
            log_file = binlog_event.next_binlog
            log_pos = binlog_event.position

            # 第一次会默认读取 binlog 的开始位置
            if log_file > self._rt.get('log_file', ''):
                rt = {
                    'log_file': log_file,
                }

                if self._rt.get('log_pos'):
                    rt.update(log_pos=log_pos)

                self._rt.update(rt)
                logging.info(f'all_rt: {self._rt}, new_rt:{rt}')
            return

        # 过滤掉不需要的表：最近一次移除的
        key = f'{binlog_event.schema}.{binlog_event.table}'
        if key not in self.inc_tables:
            return
        # 过滤掉不需要的表：上次中断的历史数据尚未补齐，新表数据不新增
        if not self.has_aligned and key in self.new_tables:
            return

        # # 过滤掉不需要的数据
        if self.is_resume:
            cached_ts = self._ts[key]
            ts = binlog_event.timestamp
            # 先判断时间戳是否满足条件
            if ts < cached_ts.time:
                # logger.info(f'Expired msg, table={key},event timestamp={ts},'
                #             f'cached timestamp={cached_ts}')
                # logger.info(f'db={binlog_event.schema}, '
                #             f'table={binlog_event.table}, '
                #             f'rows={binlog_event.rows}')
                return

        self._rt.update({
            'resume_stream': int(True),
            'log_pos': binlog_event.packet.log_pos,
        })
        logger.info(self._rt)
        yield from self.parse_binlog_rows(binlog_event)

    def parse_binlog_rows(self, binlog_event):
        """按 row 解析 binlog"""
        database = binlog_event.schema
        table = binlog_event.table
        key = f'{database}.{table}'
        topic = f'{database}-{table}'

        ts = binlog_event.timestamp
        last_row = len(binlog_event.rows)

        for i, row in enumerate(binlog_event.rows):
            offset = i + 1
            # 判断偏移量是否满足
            if self.is_resume and offset < self._ts[key].inc:
                logger.info(f'Expired msg, table={key},event timestamp={ts},'
                            f'offset={offset},cached timestamp={self._ts[key]}')
                continue

            # 组装event
            event = dict(database=database, table=table, topic=topic, ts=ts)
            if last_row == offset:
                event['commit'] = True
                offset = 0
            else:
                event['offset'] = offset

            if isinstance(binlog_event, DeleteRowsEvent):
                event['type'] = 'delete'
                event['data'] = dict(row['values'])
            elif isinstance(binlog_event, UpdateRowsEvent):
                event['type'] = 'update'
                event['old'] = dict(row['before_values'])
                event['data'] = dict(row['after_values'])
            elif isinstance(binlog_event, WriteRowsEvent):
                event['type'] = 'insert'
                event['data'] = dict(row['values'])

            yield event
            self._ts[key] = Timestamp(ts, offset)

    def disconnect(self, *args, **kwargs):
        if self.stream:
            self.stream.close()
            logger.info('MySQL stream closed')
        self.conn.close()
        logger.info('MySQL connector closed')


if __name__ == '__main__':
    import traceback
    from config.db_config import MYSQL_R_CONFIG
    from utils.log import configure_logging

    configure_logging()
    cf = MYSQL_R_CONFIG

    # 第一次读取
    # ks = ['statistics.StarUser2']
    # r = MySQLReader(ks, **cf)
    #
    # try:
    #     for n in r.read():
    #         print(json.dumps(n, cls=DateEncoder))
    # except KeyboardInterrupt:
    #     traceback.print_exc()
    # except Exception:
    #     traceback.print_exc()
    # finally:
    #     r.stop()
    #
    # time.sleep(30)

    # 第二次读取，新增表
    ks = ['statistics.StarUser2', 'statistics.StarUser1']
    # ks = ['statistics.StarUser1']
    r = MySQLReader(ks, **cf)
    # r = MySQLReader(ks, **cf, is_resume=False, is_bootstrap=False)

    try:
        for n in r.read():
            print(json.dumps(n, cls=DateEncoder))
            r.commit()
    except KeyboardInterrupt:
        traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        r.stop()


    def fa(a):
        for i in range(a):
            yield i

    def fb(b):
        if b == 1:
            return
        else:
            yield from fa(b)

    def fc(c):
        yield from fb(c)
