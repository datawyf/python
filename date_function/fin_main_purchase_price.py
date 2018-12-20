# -*- coding: utf-8 -*-
# author : wyf
'''
#需求描述:财务主表计算所需要的采购价
表：dw.fin_main_purchase_price
记录入库的imei的采购价等信息
'''
import time, MySQLdb
import dbSetting as db
import sys

# 建立目标数据库链接
toconn = MySQLdb.connect(host=db.toDb['ip'], user=db.toDb['user'], passwd=db.toDb['passwd'], db=db.toDb['dbname'],
                         port=db.toDb['port'], charset=db.toDb['charset'])
tocursor = toconn.cursor()

if __name__ == "__main__":
    try:
        # 判断脚本是否传入参数
        sys1 = sys.argv[1]
        dt_created_start = "'" + sys1 + "'"
        time_created_start = "'" + sys1 + " 00:00:00'"
    except:
        dt_created_start = "date_sub(date(now()),interval 30 day)"
        time_created_start = "concat(date_sub(date(now()),interval 30 day),' 23:59:59')"

    try:
        sys2 = sys.argv[2]
        dt_created_end = "'" + sys2 + "'"
        time_created_end = "'" + sys2 + " 23:59:59'"
    except:
        dt_created_end = "date(now())"
        time_created_end = "concat(date(now()),' 23:59:59')"

    dt_created_where = "between " + dt_created_start + " and " + dt_created_end
    time_created_where = "between " + time_created_start + " and " + time_created_end
    print(dt_created_where, time_created_where)

    sqls = '''
    drop table if exists dw.fin_main_purchase_price_wtmp;
    create table dw.fin_main_purchase_price_wtmp like dw.fin_main_purchase_price
    '''
    for sql in sqls.split(";"):
        print(sql)
        tocursor.execute(sql)
        toconn.commit()
    # 采购入库
    sql = '''
    INSERT INTO `dw`.`fin_main_purchase_price_wtmp`( `imei`, `id_sku`, `unit_price`, `in_type`, `supplier_name`, `sku_name`, `invoice_type`, `tax_rate`, `dt_created`)
    SELECT a.imei,d1.id as id_sku,a.`unit_price` as unit_price,0 as in_type,
            d.`supplier_name`,d1.name as sku_name,c.invoice_type,
            c.tax_rate/10000 as tax_rate,
            a.`dt_created` 
            FROM ods.`airent_tbl_supply_order_detail` a
            LEFT JOIN  ods.`airent_e_purchase_item` b
            ON a.purchase_item_id=b.id
            LEFT JOIN ods.`airent_e_purchase` c
            ON b.`purchase_id`=c.`id`
            LEFT JOIN ods.`airent_tbl_supply_order` d
            ON a.`id_supply_order`=d.`id`
            LEFT JOIN ods.airent_tbl_sku d1
            ON a.id_sku=d1.id
            WHERE date(a.dt_created) ''' + dt_created_where + ''' 
            AND a.quality_status=2
            AND a.status=2;
    '''
    print(sql)
    tocursor.execute(sql)
    toconn.commit()

    # 还机入库
    sql = '''
    INSERT INTO `dw`.`fin_main_purchase_price_wtmp`( `imei`, `id_sku`, `unit_price`, `in_type`, `supplier_name`, `sku_name`, `invoice_type`, `tax_rate`, `dt_created`)
    SELECT b2.imei,b2.id_sku as id_sku,
        CASE WHEN m.finance_price=0 THEN m.price/100 ELSE m.finance_price/100 END as unit_price,
        1 as in_type, # 还机入库
        d.supplier_name,d1.name as sku_name,
        0 as invoice_type,
        0 as tax_rate,
        b.dt_created 
        FROM ods.airent_tbl_stock_mvt b
        LEFT JOIN ods.airent_tbl_stock b2
        ON b.id_stock=b2.id
        LEFT JOIN ods.`airent_tbl_supply_order_detail` b3
        ON b2.imei=b3.imei
        AND b3.quality_status=2
        AND b3.status=2
        LEFT JOIN ods.`airent_tbl_supply_order` d
        ON b3.`id_supply_order`=d.`id`
        LEFT JOIN ods.airent_tbl_sku d1
        ON b3.id_sku=d1.id
        LEFT JOIN ods.`airent_tbl_return_detail` c1
        ON c1.id=b.id_order_return_detail
        LEFT JOIN ods.airent_e_stock_transform m
        ON b2.`imei`=m.imei
        WHERE b.dt_created ''' + time_created_where + '''
        and b.id_stock_mvt_reason  IN (13)
        and m.type=1 and m.status=3 
    '''
    print(sql)
    tocursor.execute(sql)
    toconn.commit()
    ## 其他转换单入库数据
    sql = '''
        select a.*
        FROM ods.airent_e_stock_io_record a
        LEFT JOIN ods.airent_e_after_sale b ON a.receipt_number=b.sn
        left join dw.fin_main_purchase_price c on b.imei = c.imei and a.dt_created >c.dt_created
         WHERE a.in_stock_type in (1,2,3,4,6) and a.imei <> b.imei
         group by a.imei having count(1)>1;

         select a.*,b.imei,c.dt_created
        FROM ods.airent_e_stock_io_record a
        LEFT JOIN ods.airent_e_after_sale b ON a.receipt_number=b.sn
        left join dw.fin_main_purchase_price c on b.imei = c.imei and a.dt_created >c.dt_created
         WHERE a.in_stock_type in (1,2,3,4,6) and a.imei <> b.imei
         and a.imei in ('354841092854685','354850095190872')

    select *
from dw.fin_main_purchase_price
where (imei,dt_created) in (
SELECT c.imei,max(c.dt_created) as dt_created 
FROM ods.airent_e_stock_io_record a
LEFT JOIN ods.airent_e_after_sale b ON a.receipt_number=b.sn
left join dw.fin_main_purchase_price c on b.imei = c.imei and a.dt_created >c.dt_created
 WHERE a.in_stock_type in (1,2,3,4,6) and a.imei <> b.imei
 group by c.imei)
 '''

    ## 插入前删除
    sqls = '''
    delete from dw.fin_main_purchase_price where dt_created ''' + time_created_where + ''';
    insert into dw.fin_main_purchase_price 
    select *
    from dw.fin_main_purchase_price_wtmp 
    '''
    for sql in sqls.split(";"):
        print(sql)
        tocursor.execute(sql)
        toconn.commit()

# 断开数据库链接
toconn.close()