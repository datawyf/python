#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pika

list_test=['airent','airent','tbl_sku','','id','ods','ods','airent_tbl_sku']
print(type(list_test))
arg = ";".join(list_test)

# arg=sys.argv[1]
# bodyarg 包含：source_instance,source_db,source_table,where,keyname,target_instance,target_db,target_table


# ######################### 生产者 #########################
credentials = pika.PlainCredentials('fishre', 'yu123456')
#链接rabbit服务器（localhost是本机，如果是其他服务器请修改为ip地址）
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.119',5672,'/',credentials))
#创建频道
channel = connection.channel()
# 声明消息队列，消息将在这个队列中进行传递。如果将消息发送到不存在的队列，rabbitmq将会自动清除这些消息。如果队列不存在，则创建
channel.queue_declare(queue='wyf')
#exchange -- 它使我们能够确切地指定消息应该到哪个队列去。
# #向队列插入数值 routing_key是队列名 body是要插入的内容
# channel.exchange_declare(exchange='logs',
#                          exchange_type='fanout')
# message = ' '.join(sys.argv[1:]) or "info: Hello World!"
# channel.basic_publish(exchange='logs',
#                       routing_key='',
#                       body=message)
# print(" [x] Sent %r" % message)
# connection.close()

channel.basic_publish(exchange='',
                  routing_key='wyf',  #默认的exchange routing_key 指定了 收取的队列名称
                  body=arg)
print("开始队列")
#缓冲区已经flush而且消息已经确认发送到了RabbitMQ中，关闭链接
connection.close()