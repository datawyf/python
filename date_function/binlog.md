基于binlog的增量订阅与消费
消费方式：
https://www.cnblogs.com/zhangjianhua/p/8080538.html

119测试机器给你启动一个mysql5.6以及 hbase
hbase 用线上的
binlog 最后写入 hbase
可以直接用stream1 直接操作


http://gitlab.xianghuanji.com/fan.yu/xhj/blob/master/%E5%AE%9E%E6%97%B6%E6%B5%81/hbasetest/test.py


http://gitlab.xianghuanji.com/xi.he/mysql-server/tree/binlog


写入hbase 的目的和意义是？
对业务库变动的监控？
监控表中的字段发生变化后，但是dt_updated 没有发生变化？