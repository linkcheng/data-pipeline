#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: mysql_writer 
@date: 2019-06-04 
"""
import logging
from collections import deque

from pymysql.cursors import DictCursor
from connectors.mysql_connector import MySQLConnector
from writers.base import BaseWriter
from utils.sql_builder import RowDataSQLBuilder

logger = logging.getLogger(__name__)


class MySQLWriter(BaseWriter):

    def __init__(self, db_name, table_name, *, host, port, user, password, charset,
                 database=None, autocommit=False, db=None,
                 cursorclass=DictCursor):
        self.db_name = db_name
        self.table_name = table_name
        # sql 构造器
        logger.info(f'RowDataSQLBuilder({db_name, table_name})')
        self.builder = RowDataSQLBuilder(db_name, table_name)
        self.conn = MySQLConnector(host=host, port=port, user=user,
                                   password=password, charset=charset,
                                   database=database, autocommit=autocommit,
                                   db=db, cursorclass=cursorclass)
        # sql 与 参数的汇总
        # 格式[(sql1, [args1]), (sql2, [args21, args22])]
        self.sql_args = deque(maxlen=self.maxlen)

    def write(self, sql: str, args):
        """\
        相同 SQL 语句的参数合并一起执行
        :param sql:
        :param args:
        :return:
        """
        # 最近两次的 sql 相同，则表示参数可以合并
        if self.sql_args and self.sql_args[-1][0] == sql:
            self.sql_args[-1][1].append(args)

            if len(self.sql_args[-1][1]) > self.maxlen:
                self.commit()
        else:
            self.sql_args.append((sql, [args]))

        if len(self.sql_args) > self.maxlen:
            self.commit()

    def commit(self):
        """提交，真正执行 sql"""
        for sql, args in self.sql_args:
            self.executemany(sql, args)
        self.sql_args.clear()

    def insert(self, data, keys=None, db_name=None, table_name=None,
               *args, **kwargs):
        sql, _args = self.builder.insert(data, keys, db_name, table_name)
        self.write(sql, _args)
        return sql, _args

    def update(self, data, old, keys=None, db_name=None, table_name=None,
               *args, **kwargs):
        sql, _args = self.builder.update(data, old, keys, db_name, table_name)
        self.write(sql, _args)
        return sql, _args

    def delete(self, data, keys=None, db_name=None, table_name=None,
               *args, **kwargs):
        sql, _args = self.builder.delete(data, keys, db_name, table_name)
        self.write(sql, _args)
        return sql, _args

    def execute(self, sql, args):
        """执行 sql"""
        try:
            result = self.conn.execute(sql, args)
        except Exception as e:
            logger.error(f'exception={e}, sql={sql}, args={args})')
            self.conn.rollback()
            raise e
        else:
            self.conn.commit()
        return result

    def executemany(self, sql, args):
        """执行 sql"""
        try:
            result = self.conn.executemany(sql, args)
        except Exception as e:
            logger.error(f'exception={e}, sql={sql}, args={args})')
            self.conn.rollback()
            raise e
        else:
            self.conn.commit()
        return result

    def stop(self):
        super().stop()
        self.conn.close()
        logger.info(f'MySQL writer stopped')


if __name__ == '__main__':

    cfg = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '12345678',
        'db': 'statistics',
        'charset': 'utf8',
    }
    d = 'statistics'
    t = 'StarUser'
    mw = MySQLWriter(d, t, **cfg)

    i_d = {
        "id": 1,
        "name": None,
        "mobile": "15888888888",
        "gender": "other",
        "person_id": 0,
        "utm_source": "小白信用分",
        "created_time": "2019-05-18 17:59:59",
        "updated_time": "2019-05-18 17:59:59",
    }

    # i_s = mw.insert(i_d, list(i_d.keys()))
    # print(i_s)

    i_d = {
        "id": 2,
        "name": 'hahaha',
        "mobile": "13596001111",
        "person_id": 0,
        "gender": "other",
        "utm_source": "QD-ZC-EP04",
        "created_time": "2019-05-25 13:47:38",
        "updated_time": "2019-05-25 13:47:38",
    }

    # i_s = mw.insert(i_d, list(i_d.keys()))
    # print(i_s)
    # mw.commit()

    u_d = {
        "id": 1,
        "name": 'hello world',
        "mobile": "15888888888",
        "gender": "other",
        "person_id": 0,
        "utm_source": "小白信用分",
        "created_time": "2019-05-18 17:59:59",
        "updated_time": "2019-05-18 17:59:59",
    }

    u_o = {
        "name": None,
    }

    u_d1 = {
        "id": 2,
        "name": 'hahaha',
        "mobile": "13596001111",
        "person_id": 100,
        "gender": "other",
        "utm_source": "QD-ZC-EP04",
        "created_time": "2019-05-25 13:47:38",
        "updated_time": "2019-05-30 16:52:42",
    }
    u_o1 = {
        "person_id": 0,
        "updated_time": "2019-05-25 13:47:38"
    }
    # u_s = mw.update(u_d, u_o, list(u_d.keys()))
    # print(u_s)
    # mw.commit()

    d_d = {
        "id": 1,
        "name": None,
        "mobile": "15888888888",
        "gender": "other",
        "person_id": 0,
        "utm_source": "小白信用分",
        "created_time": "2019-05-18 17:59:59",
        "updated_time": "2019-05-18 17:59:59",
    }
    # d_s = mw.delete(d_d, list(d_d.keys()))
    # print(d_s)
    # mw.commit()
