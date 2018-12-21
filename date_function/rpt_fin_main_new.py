# -*- coding: utf-8 -*-
# author : wyf
#需求描述:财务主表
'''
设计两张表，一张是财务大表涉及的基础数据表 dw.fin_main_base
一张是，供报表查询的财务大表，和之前保持一致 rpt.rpt_fin_main
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
        closing_date = "date('"+sys1+"')"
    except:
        closing_date = "date(now())"

    print(closing_date)

    # 由于fin_main_purchase_price 可能会发生变化，故订单和imei采购价的关系，不固化
    # 订单和imei采购价关系表
    sql="    drop table if exists dw.order_unit_price_wtmp;"
    tocursor.execute(sql)
    toconn.commit()
    sql="""
    create table dw.order_unit_price_wtmp
    select a.id,ifnull(b.imei,'') as imei,ifnull(max(c.dt_created),'0000-00-00 00:00:00') as dt_created
    FROM dw.fin_main a
    LEFT JOIN dw.`delivery_daily` b ON a.id=b.id_trade
    LEFT JOIN dw.fin_main_purchase_price c ON b.imei=c.imei and b.dt_delivered > c.dt_created
    group by a.id,b.imei
    """

    tocursor.execute(sql)
    toconn.commit()
    sql = """
        ALTER TABLE dw.order_unit_price_wtmp ADD PRIMARY KEY ( `id` )
    """
    tocursor.execute(sql)
    toconn.commit()
    ## 订单在结账日前的实付金额记录
    sqls = """
    drop table if exists dw.order_receipt_wtmp;
    CREATE TABLE dw.`order_receipt_wtmp` (
      `trade_id` int(10) unsigned DEFAULT '0',
      `amount` decimal(33,2) DEFAULT NULL,
      KEY `index_1` (`trade_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    insert into dw.order_receipt_wtmp
     select c.id as trade_id,sum(ifnull(a.already_pay_amount,0)+ifnull(a.left_pay_amount,0)) as amount
    FROM ods.`airent_tbl_installment_pay_plan` a
    left join ods.airent_e_sub_trade b on a.trade_no = b.sub_trade_no
	left join dw.fin_main c on b.main_trade_no = c.trade_no
    where a.is_finished =1 and a.finished_time < concat("""+closing_date+""",' 00:00:00')  and a.is_delete = 0 and c.trade_no is not null
    group by c.id
    """
    for sql in sqls.split(";"):
        print(sql)
        tocursor.execute(sql)
        toconn.commit()
    sqls="""
    drop table if exists dw.fin_main_base_wtmp;
    create table dw.fin_main_base_wtmp like dw.fin_main_base
    """
    for sqlper in sqls.split(';'):
        print sqlper
        tocursor.execute(sqlper)
        toconn.commit()
    sql = """
INSERT INTO  dw.fin_main_base_wtmp (`trade_id`, `trade_no`, `user_id`, `dt_created`, `cash`, `order_version`, `freeze_cash`,
`sp_reduce_flag`, `installments_sum`, `choose_installments_num`, `id_sku`, `contract_charge`, `contract_reduce_charge`,
 `assurance_charge`, `assurance_reduce_charge`, `subsidy_charge`, `lbf_price`, `service_charge`,`pay_code`, `purchase_price`, `imei`,
  `supplier_name`, `sku_name`, `invoice_type`, `tax_rate`, `dt_signed`, `dt_delivered`, `renew_end_date`,
   `contract_receipt`, `tail_receipt`, `residual_value`, `rent_month`, `income_month`, `service_income`, `rental_service_income`,
    `rental_equipment_income`, `cost_month`, `closing_date`, `cal_date`, `rent_months_total`, `rent_monts_cur_year`, `order_status`)

SELECT a.id as trade_id,a.trade_no,a.id_user ,a.dt_created,a.cash ,a.version as order_version,a.freeze_cash
  ,a.flag  as sp_reduce_flag,a.installments_sum ,a.choose_installments_num ,a.id_sku,a.contract_charge ,a.contract_reduce_charge
  ,a.assurance_charge,a.assurance_reduce_charge,a.subsidy_charge,a.lbf_price ,a.service_charge,e4.pay_code
  ,(CASE WHEN e2.channel_id = 33 THEN el1.official_price/100 ELSE  c.unit_price  END) as purchase_price,ou.imei
  ,(CASE WHEN e2.channel_id = 33 THEN '魅族品牌馆' ELSE  c.supplier_name  END)   as supplier_name
  ,IFNULL(c.sku_name,el1.name)  as sku_name
  ,CASE WHEN e2.channel_id = 33 THEN 1 ELSE c.invoice_type END as invoice_type
  ,(CASE WHEN e2.channel_id = 33 THEN 0.16 ELSE c.tax_rate END) as tax_rate
  ,e2.dt_signed,e3.dt_delivered,mmmm1.renew_end_date

, CASE WHEN max(k.status) IN (3,4,5) THEN 0
WHEN max(k.status) IN (8,15,10,26,30,31) THEN d.amount
WHEN max(k.status) IN (27,28) THEN a11.contract_charge
ELSE 0 END as contract_receipt


, CASE WHEN max(k.status) IN (3,4,5) THEN 0
WHEN max(k.status) IN (8,15,10,26,30,31) THEN 0
WHEN max(k.status) IN (27,28) THEN a11.assurance_charge
ELSE 0 END as tail_receipt


,mmm.max_price/100 as residual_value
,(a.contract_charge-a.contract_reduce_charge+a.service_charge)/a.choose_installments_num as rent_month # （总租金+意外保障服务费-减免）/总租期
,((a.contract_charge-a.contract_reduce_charge+a.service_charge)/a.choose_installments_num) / 2 / (1+(0.16 ))
+((a.contract_charge-a.contract_reduce_charge+a.service_charge)/a.choose_installments_num) / 2 /1.06  as income_month # 每月租金/2/1.06+每月租金/2/1.16
,a.service_charge/1.06 as service_income #意外保障服务费/1.06
,(a.contract_charge-a.service_charge-a.contract_reduce_charge)/2/1.06/a.choose_installments_num as rental_service_income #每月租赁服务收入 = （总租金-意外保障服务费-租金减免）/总租期/2/1.06
,(a.contract_charge+a.service_charge-a.contract_reduce_charge)/2/1.16/a.choose_installments_num as rental_equipment_income #每月租赁设备收入 = （总租金+意外保障服务费-租金减免）/总租期/2/1.16
,((CASE WHEN e2.channel_id = 33 THEN el1.official_price/100 ELSE  c.unit_price  END)-mmm.max_price/100) / (1+((CASE WHEN e2.channel_id = 33 THEN 0.16 ELSE (CASE WHEN c.invoice_type=2 AND c.dt_created>='2018-05-01' THEN 0.16 WHEN c.invoice_type=2 AND c.dt_created<'2018-05-01' THEN 0.17  ELSE 0 END) END) )) /a.choose_installments_num as cost_month
,
""" + closing_date +""" as  closing_date
,CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END as cal_date


,least(a.choose_installments_num,CASE WHEN  e2.dt_signed IS NULL THEN 0
WHEN """ + closing_date +""">(CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END) THEN TIMESTAMPDIFF(MONTH,e2.dt_signed,CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END )+1
ELSE TIMESTAMPDIFF(MONTH,e2.dt_signed,CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END )+0.5  END) as  rent_months_total

,least(month(""" + closing_date +"""),(CASE WHEN  e2.dt_signed IS NULL THEN 0
WHEN """ + closing_date +""">(CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END) THEN TIMESTAMPDIFF(MONTH,e2.dt_signed,CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END )+1
ELSE TIMESTAMPDIFF(MONTH,e2.dt_signed,CASE WHEN """ + closing_date +""">DATE(mmmm1.renew_end_date) THEN DATE(mmmm1.renew_end_date) ELSE """ + closing_date +""" END )+0.5  END)) as rent_monts_cur_year
,max(k.status) AS status

FROM dw.fin_main a
left join dw.order_unit_price_wtmp ou on a.id = ou.id
LEFT JOIN dw.`fin_main_purchase_price` c
ON ou.imei=c.imei and ou.dt_created = c.dt_created
# LEFT JOIN dw.`product_sku_main` e
# ON a.id_sku=e.id_sku
LEFT JOIN ods.airent_e_trade e2
ON a.id=e2.id
left join ods.airent_e_sub_trade e4 on e2.trade_no = e4.sub_trade_no
LEFT JOIN dw.`delivery_daily_oldest` e3
ON a.id=e3.id_trade
LEFT JOIN dw.order_pay_balance  a11
ON a.id=a11.id
left join dw.order_receipt_wtmp d on a.id = d.trade_id
LEFT JOIN ods.product_e_sku el1
ON a.id_sku = el1.id
AND el1.is_active = 1
LEFT JOIN ods.`airent_e_sub_rent_trade_extend` mmmm1
ON a.trade_no=mmmm1.sub_trade_no
LEFT JOIN ods.`airent_e_residual_value` mmm
ON a.id_sku=mmm.sku_id
AND mmm.month=REPLACE(SUBSTR((DATE_ADD(e3.dt_delivered,INTERVAL a.choose_installments_num  MONTH)),1,7),'-','')
LEFT JOIN dw.`order_status_l` k
  ON a.id=k.id_trade
  AND  k.start_date<=""" + closing_date +""" AND k.end_date>""" + closing_date +"""
where e2.status  not in(1,2,3,4,5,7,72)
-- where e2.dt_signed< """ + closing_date +"""
GROUP BY 1
"""
    tocursor.execute(sql)
    toconn.commit()
    ## 插入前删除
    sqls="""
    delete from dw.fin_main_base where closing_date in (select distinct closing_date from dw.fin_main_base_wtmp);
    insert into dw.fin_main_base select * from dw.fin_main_base_wtmp;
    # 插入到rpt.rpt_fin_main
    delete from rpt.rpt_fin_main_wyf where 结账日期 in (select distinct closing_date from dw.fin_main_base_wtmp);
    INSERT INTO  rpt.rpt_fin_main_wyf (`订单id`,`订单号`,`用户id`,`下单时间`,`原始押金`,`订单类型(1新 0老)`,`冻结金额`,`特殊减免标识`,
    `老订单总押金`,`初始选择租期`,`idsku`,`初始租金`,`初始租金减免`,`初始买断价`,`初始押金减免`,`初始爱回收补贴`,`初始乐百分手续费`,
    `初始保险金额`,`支付方式`,`采购金额`,`imei`,`供应商名称`,`采购机型`,`发票类型`,`税率`,`不含税采购价`,`签收日期`,`发货日期`,`订单结束日期`,
    `实收租机租金金额`,`实收尾款金额`,`残值`,`增值税`,`每月租金`,`每月收入`,`意外保障收入`,`租赁服务收入`,`租赁设备收入`,
    `累计意外保障收入`,`累计租赁服务收入`,`累计租赁设备收入`,`每月成本`,`结账日期`,`计算日`,`已租月份`,`累计租金`,`累计收入`,
    `累计成本`,`其他非流动资产`,`应收账款`,`递延收益old`,`递延收益`,`当年已租月份`,`当年累计租赁服务收入`,`当年累计租赁设备收入`,`当年累计成本`,`订单状态代码`)
    select a.trade_id,a.trade_no,a.user_id,a.dt_created,a.cash,a.order_version,a.freeze_cash,a.sp_reduce_flag,
    a.installments_sum,a.choose_installments_num,a.id_sku,a.contract_charge,a.contract_reduce_charge,a.assurance_charge,a.assurance_reduce_charge,
    a.subsidy_charge,a.lbf_price,a.service_charge,
    CASE WHEN a.pay_code IN('pas08','pas16') THEN '乐百分'
    WHEN a.pay_code='pas15' THEN '还呗'
    ELSE '花呗' END "支付方式",
    a.purchase_price,a.imei,a.supplier_name,a.sku_name,
    case when a.invoice_type =1 then "增值税普通发票" when a.invoice_type =2 then "增值税专用发票" else "unknow" end "发票类型",
    a.tax_rate,
    a.purchase_price/(1+a.tax_rate) as "不含税采购价",# 不含税采购价 = 采购金额/（1+税率）
    a.dt_signed,a.dt_delivered,a.renew_end_date,
    a.contract_receipt,a.tail_receipt,a.residual_value,
    a.service_income*0.06+(a.rental_service_income *a.rent_months_total)*0.06+a.rental_equipment_income*a.rent_months_total*0.16 as "增值税", # 增值税=累计增值税，意外保障服务费*0.06+累计租赁服务收入*0.06+累计租赁设备收入*0.16
    a.rent_month,a.income_month,a.service_income,a.rental_service_income,a.rental_equipment_income,
    a.service_income as "累计意外保障收入",# 累计意外保障收入 = 同意外保障收入
    a.rental_service_income *a.rent_months_total as "累计租赁服务收入",# 累计租赁服务收入 =  实际确认收入=每月租赁服务收入*已租月份
    a.rental_equipment_income*a.rent_months_total as "累计租赁设备收入", # 累计租赁设备收入 = 每月租赁设备收入*已租月份
    a.cost_month as "每月成本",
    a.closing_date as "结账日期",
    a.cal_date as "计算日",
    a.rent_months_total as "累计已租月份",
    a.rent_month * a.rent_months_total as "累计租金", # 累计租金 =AG每月租金*已租月份
    a.income_month * a.rent_months_total as "累计收入", # 累计收入 = 每月收入*已租月份
    a.cost_month * a.rent_months_total as "累计成本",# 累计成本 = 已租月份*AO每月成本
    a.purchase_price/(1+a.tax_rate)-(a.cost_month * a.rent_months_total) as "其他非流动资产",# 其他非流动资产 = 采购价/（1+税率）-累计成本
    greatest(0,a.rent_month * a.rent_months_total-contract_receipt) as "应收账款",
    greatest(0,contract_receipt-a.rent_month * a.rent_months_total) as "递延收益old",
    a.contract_receipt-a.service_income*1.06-(a.rental_service_income *a.rent_months_total)*1.06-a.rental_equipment_income*a.rent_months_total*1.16 as "递延收益", # 递延收益 = 实收租机租金金额-意外保障服务费*1.06-累计租赁服务收入*1.06-累计租赁设备收入*1.16
    a.rent_monts_cur_year as "当年已租月份",
    a.rent_monts_cur_year*a.rental_service_income as "当年累计租赁服务收入", # 当年累计租赁服务收入 = 当年已租月份*AJ 每月租赁服务收入
    a.rent_monts_cur_year * a.rental_equipment_income as "当年累计租赁设备收入", #当年累计租赁设备收入 = 当年已租月份*AK 每月租赁设备收入
    a.rent_monts_cur_year * a.cost_month as "当年累计成本", # 当年累计成本 = 每月成本AO*当年已租月份
    a.order_status
    from dw.fin_main_base_wtmp a;

    drop table if exists dw.fin_main_base_wtmp;
    drop table if exists dw.order_unit_price_wtmp
    """
    for sqlper in sqls.split(';'):
        # print sqlper
        tocursor.execute(sqlper)
        toconn.commit()
#断开数据库链接
toconn.close()