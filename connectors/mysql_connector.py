#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link
@contact: zhenglong1992@126.com
@module: mysql_connector
@date: 2019-06-03
"""
import pymysql
from pymysql.cursors import SSCursor, SSDictCursor, DictCursor
from pymysql.err import Error, OperationalError


class MySQLConnector:
    def __init__(self, cursorclass=DictCursor, **kwargs):
        kwargs['cursorclass'] = cursorclass
        self.conn = pymysql.connect(**kwargs)
        self.cr = self.conn.cursor()

    def __getattr__(self, item):
        return getattr(self.conn, item)

    def __del__(self):
        self.close()

    def execute(self, sql, args=None):
        try:
            result = self.cr.execute(sql, args)
        except OperationalError:
            self.conn.ping()
            result = self.cr.execute(sql, args)
        return result

    def executemany(self, sql, args=None):
        try:
            result = self.cr.executemany(sql, args)
        except OperationalError:
            self.conn.ping()
            result = self.cr.executemany(sql, args)
        return result

    def fetchall(self):
        return self.cr.fetchall()

    def fetchall_unbuffered(self):
        return self.cr.fetchall_unbuffered()

    def close(self):
        try:
            self.cr.close()
            self.conn.close()
        except Error:
            pass

    def read(self, sql: str, args=None, buffered=True):
        """统一以列表生成式返回数据"""
        self.execute(sql, args)
        if self.conn.cursorclass in (SSCursor, SSDictCursor):
            fetch = self.cr.fetchall if buffered else self.cr.fetchall_unbuffered
        else:
            fetch = self.cr.fetchall

        yield from fetch()

    def get_create_table(self, database, table):
        """加载数据表 schema 信息 """
        schema = ''
        sql = f'show create table {database}.{table}'
        ret = list(self.read(sql))
        if ret:
            schema = ret[0].get('Create Table') or ''
        return schema

    def get_columns_type(self, database, table):
        """加载数据表各列数据类型"""
        sql = f"""
             select 
                 column_name
                 ,data_type
             from information_schema.columns 
             where table_schema='{database}' and table_name='{table}'
         """
        ret = self.read(sql)
        return {item['column_name']: item['data_type'] for item in ret}

    def get_tx_isolation(self):
        tx_isolation = ''
        sql = 'select @@session.tx_isolation as tx_isolation'
        ret = list(self.read(sql))
        if ret:
            tx_isolation = ret[0].get('tx_isolation') or ''
        return tx_isolation

    def set_tx_isolation(self, tx_isolation):
        sql = f"set session tx_isolation = '{tx_isolation}'"
        self.execute(sql)

    def start_transaction(self, with_snapshot=False):
        if with_snapshot:
            # /*!40100 ...*/ 这部分注释在 MySQL 服务端版本号大于4.1.00时执行
            sql = 'start transaction with consistent snapshot'
        else:
            sql = 'start transaction'
        self.execute(sql)

    def get_global_gtid_executed(self):
        gtid_executed = ''
        sql = 'select @@global.gtid_executed as gtid_executed'
        ret = list(self.read(sql))
        if ret:
            gtid_executed = ret[0].get('gtid_executed') or ''
        return gtid_executed

    def lock_tables(self):
        sql = 'flush tables with read lock'
        self.execute(sql)

    def unlock_tables(self):
        sql = 'unlock tables'
        self.execute(sql)

    def get_binlog_file_position(self):
        sql = 'show master status;'
        ret = list(self.read(sql))
        return {
            'log_file': ret[0].get('File') or '',
            'log_pos': ret[0].get('Position') or 0,
        }


if __name__ == '__main__':
    import time
    from config.db_config import MYSQL_R_CONFIG

    db = MySQLConnector(**MYSQL_R_CONFIG)

    tx = db.get_tx_isolation()
    print(tx)

    # sql = """start transaction with consistent snapshot;select @@global.gtid_executed as gtid_executed;"""
    s = """start transaction with consistent snapshot;"""
    db.executemany(s)

    s = """select @@global.gtid_executed as gtid_executed;"""
    db.cr.execute(s, None)
    r = db.fetchall()
    print(r)
    if r:
        print(r[0].get('gtid_executed'))

    db.set_tx_isolation('read-committed')
    tx = db.get_tx_isolation()
    print(tx)

    # r = db.execute("set wait_timeout=10;")
    # db.execute("set interactive_timeout=10;")
    # db.execute("select * from StarUser;")
    # values = db.cr.fetchall() or []
    # print(values)

    # time.sleep(15)
    #
    # s1 = """insert into `statistics`.`StarUser`
    #     (mobile,created_time,gender,utm_source,updated_time,app_source)
    #     values (%s,%s,%s,%s,%s,%s);"""
    # a1 = (
    #     ('15583311111', '2019-05-18 17:59:59', 'other', '小白信用分',
    #      '2019-05-18 17:59:59', ''),
    #     ('15868112315', '2019-05-19 17:59:59', 'other', '大白信用分',
    #      '2019-05-19 17:59:59', '')
    # )
    #
    # db.executemany(s1, a1)
    # db.commit()
    # db.execute("select * from StarUser;")
    # values = db.cr.fetchall() or []
    # print(values)

    db.close()
    db.close()
