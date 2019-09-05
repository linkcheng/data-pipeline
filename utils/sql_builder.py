#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: sql_builder 
@date: 2019-06-03 
"""
from functools import lru_cache
from werkzeug.utils import cached_property


class RowDataSQLBuilder:
    def __init__(self, database, table, columns=None,
                 *, imode=None, umode=None, dmode=None):
        """构造器
        :param columns: [] or None, 数据表的列
            如果为 None，表示从原数据提取
        :param imode: 在 insert 时，有几种种模式可选
            None: 原生模式
            ignore: 新数据重复忽略，依赖 unique key
            replace：新数据重复替换，生成新 id
        :param umode: 在 update 时，有几种种模式可选
            None: 原生模式，update set
            replace：替换 replace into
        :param dmode: 在 delete 时，有几种种模式可选
            None: 原生模式 delete
            ignore: 忽略删除
        """
        self.database = database
        self.table = table
        self.imode = imode
        self.umode = umode
        self.dmode = dmode
        self.columns = columns

    @cached_property
    def join_columns(self):
        """ '`name`, `gender`, `age`' 格式"""
        return ', '.join((f'`{col}`' for col in self.columns))

    @cached_property
    def join_percents(self):
        """ '%s, %s, %s' 格式"""
        return ', '.join(['%s'] * len(self.columns))

    @cached_property
    def join_columns_percents_with_comma(self):
        """ '`name`=%s, `gender`=%s, `age`=%s' 格式"""
        return ', '.join([f'`{col}`=%s' for col in self.columns])

    @cached_property
    def join_columns_percents_with_and(self):
        """ '`name`=%s and `gender`=%s and `age`=%s' 格式"""
        return ' AND '.join([f'`{col}`=%s' for col in self.columns])

    join_where = join_columns_percents_with_and

    def join_where_with_null(self, args):
        """ '`name`=%s and `gender` is %s and `age`=%s' 格式"""
        return ' AND '.join([f'`{col}` IS %s' if args[i] is None else f'`{col}`=%s'
                             for i, col in enumerate(self.columns)])

    @lru_cache(32)
    def insert_sql(self, database=None, table=None):
        """insert sql"""
        database = database or self.database
        table = table or self.table
        if self.imode == 'ignore':
            sql = (
                f' INSERT ignore INTO `{database}`.`{table}`'
                f' ({self.join_columns}) VALUES ({self.join_percents})'
            )
        elif self.imode == 'replace':
            sql = (
                f' REPLACE INTO `{database}`.`{table}`'
                f' ({self.join_columns}) VALUES ({self.join_percents})'
            )
        else:
            sql = (
                f' INSERT INTO `{database}`.`{table}`'
                f' ({self.join_columns}) VALUES ({self.join_percents})'
            )
        return sql

    @lru_cache(32)
    def update_sql(self, database=None, table=None):
        """update sql"""
        database = database or self.database
        table = table or self.table
        if self.umode == 'replace':
            sql = (
                f' REPLACE INTO `{database}`.`{table}`'
                f' ({self.join_columns}) VALUES ({self.join_percents})'
            )
        else:
            sql = (
                f' UPDATE `{database}`.`{table}` '
                f' SET {self.join_columns_percents_with_comma} '
                f' WHERE {self.join_where} LIMIT 1'
            )
        return sql

    def update_sql_with_null(self, args, database=None, table=None):
        """update sql with null"""
        database = database or self.database
        table = table or self.table
        if self.umode == 'replace':
            sql = (
                f' REPLACE INTO `{database}`.`{table}`'
                f' ({self.join_columns}) VALUES ({self.join_percents})'
            )
        else:
            sql = (
                f' UPDATE `{database}`.`{table}` '
                f' SET {self.join_columns_percents_with_comma} '
                f' WHERE {self.join_where_with_null(args)} LIMIT 1'
            )
        return sql

    @lru_cache(32)
    def delete_sql(self, database=None, table=None):
        """delete sql"""
        database = database or self.database
        table = table or self.table
        if self.dmode == 'ignore':
            sql = f'DELETE FROM `{database}`.`{table}` WHERE 1<>1'
        else:
            sql = (
                f' DELETE FROM `{database}`.`{table}`'
                f' WHERE {self.join_where} LIMIT 1'
            )
        return sql

    def delete_sql_with_null(self, args, database=None, table=None):
        """delete sql"""
        database = database or self.database
        table = table or self.table
        if self.dmode == 'ignore':
            sql = f'DELETE FROM `{database}`.`{table}` WHERE 1<>1'
        else:
            sql = (
                f' DELETE FROM `{database}`.`{table}`'
                f' WHERE {self.join_where_with_null(args)} LIMIT 1'
            )
        return sql

    def build(self, data, keys):
        """\
        基于传入数据与关键字格式化 sql 参数
        :param data: 传入数据, dict
        :param keys: 关键字列表, list
        :return:
        """
        if not keys:
            keys = list(data.keys())

        if not self.columns:
            self.columns = keys

        return [data.get(col) for col in self.columns]

    def insert(self, data, keys=None, database=None, table=None):
        """\
        基于原始数据生成 insert sql
        :param data: 数据, dict
        {
            "id": 1,
            "name": None,
            "mobile": "15888888888",
            "gender": "other",
            "person_id": 0,
            "utm_source": "小白信用分",
            "created_time": "2019-05-18 17:59:59",
            "updated_time": "2019-05-18 17:59:59",
        }
        :param keys: None or [],
            表示从 value 中获取的数据关键字，
            如果为 None 表示使用 value 中 data 的所有 key
        :param database: None or str, 写入的目标库
        :param table: None or str, 写入的目标表
        :return: (sql: str, values: list)
        """
        args = self.build(data, keys)
        return self.insert_sql(database, table), args

    def update(self, data, old, keys=None, database=None, table=None):
        """\
        基于原始数据生成 update sql
        :param data: 全量新数据
        {
            "id": 1111111761,
            "name": None,
            "mobile": "13596001111",
            "gender": "other",
            "person_id": 100,
            "utm_source": "QD-ZC-EP04",
            "created_time": "2019-05-25 13:47:38",
            "updated_time": "2019-05-30 08:52:42",
        }
        :param keys: None or [],
            表示从 value 中获取的数据关键字，
            如果为 None 表示使用 value 中 data 的所有 key
        :param old: 被修改的旧数据
        {
            "person_id": 0,
            "updated_time": "2019-05-25 05:47:38"
        }
        :param database: None or str, 写入的目标库
        :param table: None or str, 写入的目标表
        :return: (sql: str, values: list)
        """
        new_args = self.build(data, keys)

        if self.umode == 'replace':
            return self.update_sql(database, table), new_args
        else:
            old_args = self.build({**data, **old}, keys)
            # 对于数据有 None（即数据库值为 NULL）的字段特殊处理
            if None in old_args:
                update_sql = self.update_sql_with_null(old_args, database, table)
            else:
                update_sql = self.update_sql(database, table)
            return update_sql, new_args + old_args

    def delete(self, data, keys=None, database=None, table=None):
        """\
        基于原始数据生成 delete sql
        :param data: 全量新数据
        {
            "id": 2,
            "name": None,
            "mobile": "13596001111",
            "person_id": 100,
            "gender": "other",
            "utm_source": "QD-ZC-EP04",
            "created_time": "2019-05-25 13:47:38",
            "updated_time": "2019-05-30 16:52:42",
        }
        :param keys: None or [],
            表示从 value 中获取的数据关键字，
            如果为 None 表示使用 value 中 data 的所有 key
        :param database: None or str, 写入的目标库
        :param table: None or str, 写入的目标表
        :return: (sql: str, values: list)
        """
        args = self.build(data, keys)
        # 如果有旧数据并且是使用
        if self.dmode == 'ignore':
            return self.delete_sql(database, table), []
        else:
            # 对于数据有 None（即数据库值为 NULL）的字段特殊处理
            if None in args:
                delete_sql = self.delete_sql_with_null(args, database, table)
            else:
                delete_sql = self.delete_sql(database, table)
            return delete_sql, args


if __name__ == '__main__':
    sb = RowDataSQLBuilder('mysql', 'test')
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
    print(sb.insert(i_d))
    print(sb.insert(i_d, database='demo', table='demo'))
    u_d = {
        "id": 2,
        "name": None,
        "mobile": "13596001111",
        "person_id": 100,
        "gender": "other",
        "utm_source": "QD-ZC-EP04",
        "created_time": "2019-05-25 13:47:38",
        "updated_time": "2019-05-30 08:52:42",
    }
    u_o = {
        "person_id": 0,
        "updated_time": "2019-05-25 05:47:38"
    }
    print(sb.update(u_d, u_o))
    print(sb.update(u_d, u_o, database='demo', table='demo'))
    # print(sb.delete())
