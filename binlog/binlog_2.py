#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import time
import sys
import json

from starbase import Connection


def main():
    mysql_settings = {'host': '',
                      'port': 3306, 'user': '', 'passwd': '123456'}
    stream = BinLogStreamReader(
        connection_settings=mysql_settings, #配置连接
        server_id=123454, #mysql的配置
        blocking=True,
        only_schemas=['test','mysql'],# 数据库限制
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
        resume_stream=True,
        only_tables=['test_wyf'],# 表限制
        log_file='mysql-bin.000002', log_pos=2280
    )
    for binlogevent in stream:
        print binlogevent #该字段的数据类型，存储的数据都有什么？
        print '##################'
        for row in binlogevent.rows:
            event = {"date": time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(binlogevent.timestamp)),
                     "schema": binlogevent.schema,
                     "table": binlogevent.table,
                     "log_pos": binlogevent.packet.log_pos,
                     "exec_status": binlogevent.complete}
            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event["before_values"] = dict(row["before_values"].items())
                event["after_values"] = dict(row["after_values"].items())
                event = dict(event.items())
            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event["values"] = dict(row["values"].items())
                event = dict(event.items())
            json_event = json.dumps(event)
            e=json.loads(json_event)
            ## 解析后的数据，插入hbase
            c = Connection(host='', port=20550)# ip;port
            t = c.table('binlog_test_wyf')
            columns_key = 'mysql-bin.000002'+time.strftime("%Y%m%d",time.localtime(binlogevent.timestamp))
            print(columns_key)
            data = {"binlog":e}
            print data
            # t.insert(columns_key,data)
        sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    main()