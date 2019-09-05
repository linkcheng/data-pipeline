#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
"""
@author: Link 
@contact: zhenglong1992@126.com
@module: test_mysql_binlog 
@date: 2019-07-02
export PYTHONPATH={pwd}
"""
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent
)

from pymysqlreplication.event import (
    GtidEvent,
    RotateEvent
)

from config.db_config import MYSQL_R_CONFIG

MYSQL_SETTINGS = MYSQL_R_CONFIG

only_events = [
    DeleteRowsEvent,
    WriteRowsEvent,
    UpdateRowsEvent,
    GtidEvent,
    RotateEvent
]

"""
connection_settings, 
server_id,
ctl_connection_settings=None, 
resume_stream=False,
blocking=False, 
only_events=None, 
log_file=None,
log_pos=None, 
filter_non_implemented_events=True,
ignored_events=None, 
auto_position=None,
only_tables=None, 
ignored_tables=None,
only_schemas=None, 
ignored_schemas=None,
freeze_schema=False, 
skip_to_timestamp=None,
report_slave=None, 
slave_uuid=None,
pymysql_wrapper=None,
fail_on_table_metadata_unavailable=False,
slave_heartbeat=None

"""

server_id = 6
blocking = True
only_schemas = ['statistics']
only_tables = ['StarUser1']
skip_to_timestamp = 1562501553
resume_stream = True
log_file = 'master.000001'
log_pos = 105709374


reader = BinLogStreamReader(
    connection_settings=MYSQL_SETTINGS,
    server_id=server_id,
    only_events=only_events,
    blocking=blocking,
    only_schemas=only_schemas,
    only_tables=only_tables,
    # skip_to_timestamp=skip_to_timestamp,
    # resume_stream=resume_stream,
    # log_file=log_file,
    # log_pos=log_pos,
)

for binlog_event in reader:
    if isinstance(binlog_event, RotateEvent):
        print(f'next_binlog={binlog_event.next_binlog}')
        print(f'position={binlog_event.position}')
    else:
        print(binlog_event.dump())
        print(binlog_event.timestamp)
        print(binlog_event.packet.log_pos)


reader.close()


