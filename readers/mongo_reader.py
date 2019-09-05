#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mongo_reader 
@date: 2019-06-13
@note: 需要所有库的读权限 readAnyDatabase
"""
import logging
from datetime import datetime

from bson import Timestamp
from pymongo import DESCENDING
from pymongo.read_concern import ReadConcern

from connectors.mongo_connector import MongoDBConnector
from readers.db_base import DBReader
from utils.str_utils import b2s

logger = logging.getLogger(__name__)


class MongoDBReader(DBReader):
    @staticmethod
    def decode(args):
        k, v = args
        return b2s(k), b2s(v)

    def __init__(self, tables, client_id='default', *, user, password, host, port,
                 database=None, is_bootstrap=True, is_resume=True):
        """从 MongoDB 读取数据
        :param tables: list, 要读取的库表
        :param client_id: str, 用于区分不同的客户端
        :param user: 连接 MongoDB 用户
        :param password: 连接 MongoDB 密码
        :param host: 连接 MongoDB 主机名
        :param port: 连接 MongoDB 端口号
        :param database: 要连接 MongoDB 的数据库
        :param is_bootstrap: 是否全量查询
        :param is_resume: 是与否启用断点续传
        """
        super().__init__(tables, f'mongo:{client_id}', is_bootstrap, is_resume)
        self.conn = MongoDBConnector(user=user, password=password,
                                     host=host, port=port, database=database)
        self.client_id = client_id
        self.db_names = set()
        self.coll_names = set()
        if isinstance(tables, (list, tuple, set)):
            for key in tables:
                db_name, coll_name = key.split('.')
                self.db_names.add(db_name)
                self.coll_names.add(coll_name)

        self.stream = None

    def bootstrap(self):
        """读取历史数据"""
        for key in self.new_tables:
            schema, table = key.split('.')
            yield from self._bootstrap(schema, table)
        self.commit()

    def _bootstrap(self, db_name, coll_name):
        """读取历史数据"""
        # 传递开始标志位
        yield {
            'operationType': 'bootstrap-start',
            'ns': {'db': db_name, 'coll': coll_name},
            'topic': f'{db_name}-{coll_name}',
        }

        db = self.conn.client[db_name]
        coll = db[coll_name]

        # 开启 session, 获得当前时间下最大的 _id, start_operation_time
        with self.conn.client.start_session() as session:
            with session.start_transaction(read_concern=ReadConcern('snapshot')):
                ts = Timestamp(datetime.utcnow(), 0)
                max_obj = coll.find_one(sort=[('_id', DESCENDING)])

        max_id = max_obj.get('_id')
        if max_id:
            domain = {'_id': {'$lte': max_id}}
            values = coll.find(domain)
            index = 1

            for val in values:
                data = {
                    'operationType': 'bootstrap-insert',
                    'fullDocument': val,
                    'documentKey': val['_id'],
                    'ns': {'db': db_name, 'coll': coll_name},
                    'topic': f'{db_name}-{coll_name}',
                }
                yield data
                # 添加采样数据到日志
                index += 1
                if index % 50_000 == 0:
                    logger.info(f"MongoDB index = {index}, data={data}")

        # 传递结束标志位
        yield {
            'operationType': 'bootstrap-complete',
            'ns': {'db': db_name, 'coll': coll_name},
            'topic': f'{db_name}-{coll_name}',
        }

        self._ts.update({
            f'{db_name}.{coll_name}': ts
        })

    def watch(self):
        """监控数据
        :return:
        :note: https://docs.mongodb.com/manual/reference/change-events/
               https://docs.mongodb.com/manual/changeStreams/
        """
        stream_config = self.build_stream_config()
        logging.info(stream_config)
        self.stream = self.conn.watch(**stream_config)

        for change_stream in self.stream:
            yield from self.parse_change_stream(change_stream)

    def build_stream_config(self):
        """构建 stream 的参数"""
        _data = self._rt.get('_data')
        if _data:
            resume_after = {
                '_data': _data
            }
            start_at_operation_time = None
        else:
            resume_after = None
            start_at_operation_time = min(list(self.timestamps.values()))

        return dict(
            full_document='updateLookup',
            start_at_operation_time=start_at_operation_time,
            resume_after=resume_after
        )

    def parse_change_stream(self, change_stream):
        """解析 msg"""
        # 过滤掉不需要的表
        db_name = change_stream.get('ns').get('db')
        coll_name = change_stream.get('ns').get('coll')
        topic = f'{db_name}-{coll_name}'
        key = f'{db_name}.{coll_name}'
        ts = change_stream.get('clusterTime')

        if key not in self.inc_tables:
            return

        if self.is_resume:
            cached_ts = self._ts[key]

            # 先判断时间戳是否满足条件
            if ts < cached_ts:
                logger.info(f'Expired msg, table={key},event timestamp={ts},'
                            f'cached timestamp={cached_ts}')
                return

        self._rt = self.stream._resume_token
        self._ts[key] = ts
        yield dict(**change_stream, topic=topic)

    def disconnect(self, *args, **kwargs):
        if self.stream:
            self.stream.close()
            logger.info('MongoDB stream closed')
        self.conn.close()
        logger.info('MongoDB connector closed')


if __name__ == '__main__':
    import time
    import traceback
    from config.db_config import MONGO_R_CONFIG, MONGO_R_REPORTING_TABLES
    from utils.log import configure_logging

    configure_logging()

    tabs = MONGO_R_REPORTING_TABLES
    mr = MongoDBReader(tabs, **MONGO_R_CONFIG)
    try:
        for m in mr.read():
            logger.info(m)
            mr.commit()
    except KeyboardInterrupt:
        traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        mr.stop()

    # time.sleep(30)
    #
    # tabs = [
    #     f'{MONGO_DB_NAME}.jingdongSdkLog',
    #     f'{MONGO_DB_NAME}.hangjuLog',
    # ]
    # mr = MongoDBReader(tabs, **MONGO_REPORTING_WR_CONFIG)
    # try:
    #     for m in mr.read():
    #         logger.info(m)
    # except KeyboardInterrupt:
    #     traceback.print_exc()
    # except Exception:
    #     traceback.print_exc()
    # finally:
    #     mr.stop()
