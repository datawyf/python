#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import sys
import json

def main():
    mysql_settings = {'host': '', #ip
                      'port': 3306, 'user': '', 'passwd': '123456'}


    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=mysql_settings,
                                server_id=123454,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                log_file='mysql-bin.000002', log_pos=2280,
                                blocking=False)

    for binlogevent in stream:
        binlogevent.dump()
    stream.close()


if __name__ == "__main__":
    main()