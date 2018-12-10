# -*- coding: utf-8 -*-
# author : yf reorder customer
#需求描述
import time, MySQLdb
import dbSettingodstowarehouse as db

# 建立源数据库连接
fromconn = MySQLdb.connect(host=db.airent['ip'], user=db.airent['user'], passwd=db.airent['passwd'],
                           db=db.airent['dbname'], port=db.airent['port'], charset=db.airent['charset'])
fromcursor = fromconn.cursor()

# 建立目标数据库链接
toconn = MySQLdb.connect(host=db.toDb['ip'], user=db.toDb['user'], passwd=db.toDb['passwd'], db='dw',
                         port=db.toDb['port'], charset=db.toDb['charset'])
tocursor = toconn.cursor()

def chargeUserIn(tcursor,userid):
    sql="select count(*) from user_detail where id_user='"+str(userid)+"'"
    tocursor.execute(sql)
    if tocursor.fetchone()[0]>0:
        return True
    else:
        return None
def getresult(fcursor,userid):
    #获取用户基础信息
    sql="SELECT a.id,a.dt_created,ifnull(b.utm_source,''),ifnull(b.utm_campaign,''),ifnull(b.utm_medium,''),a.is_active FROM airent_tbl_user a  LEFT JOIN `airent_tbl_user_utm_info` b ON a.id=b.user_id WHERE a.id='"+str(userid)+"'"
    fcursor.execute(sql)
    (id_user,dt_created,utm_source,utm_medium,utm_campaign,is_active)=fcursor.fetchone()
    sql = "SELECT DISTINCT IFNULL(a.name,b.name) NAME,b.identi_card FROM airent_tbl_user a LEFT JOIN airent_tbl_user_many_identi_info b ON a.id=b.user_id WHERE a.id='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (name,identi_card) = fcursor.fetchone()

    #提交订单数
    sql = "SELECT COUNT(*) FROM airent_e_trade WHERE id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (applyOrds,) = fcursor.fetchone()

    # 支付订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade  a  INNER JOIN airent_tbl_trade_log  b ON a.id=b.id_trade AND b.new_status=3 WHERE a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (payedOrds,) = fcursor.fetchone()

    # 在租订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade a WHERE a.status=10 AND a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (onLineOrds,) = fcursor.fetchone()

    # 逾期订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade a WHERE (a.status=30 OR (a.status =10 AND a.dt_end_date<NOW())) AND a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (overdueOrds,) = fcursor.fetchone()

    # 买断订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade a WHERE a.status=27 AND a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (buyoutOrds,) = fcursor.fetchone()

    # 还机订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade a WHERE a.status=28 AND a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (returnOrds,) = fcursor.fetchone()

    # 退货订单数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_trade a LEFT JOIN airent_tbl_trade_log b ON a.id=b.id_trade WHERE b.new_status=8 AND a.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (returnedOrds,) = fcursor.fetchone()

    # 续租次数
    sql = "SELECT COUNT(DISTINCT a.id) FROM airent_e_sub_trade a LEFT JOIN airent_e_trade b ON a.main_trade_no=b.trade_no WHERE a.main_trade_no<>a.sub_trade_no AND a.sub_trade_type=1 AND a.status=2 AND b.id_user='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (reorderOrds,) = fcursor.fetchone()

    # 用户注册电话
    sql = "SELECT IFNULL(phone,'') phone FROM airent_tbl_user  WHERE id='" + str(
        userid) + "'"
    fcursor.execute(sql)
    (regphone,) = fcursor.fetchone()

    # 用户收货电话
    sql = "SELECT ifnull(GROUP_CONCAT(mobile),'') phone  FROM (SELECT DISTINCT b.mobile FROM airent_e_trade a LEFT JOIN `airent_tbl_trade_delivery_address` b ON a.id=b.id_trade WHERE a.id_user='" + str(
        userid) + "') a"
    fcursor.execute(sql)
    (sendphone,) = fcursor.fetchone()

    # 用户信用套餐电话
    sql = "SELECT ifnull(GROUP_CONCAT(phone),'') phone   FROM (SELECT DISTINCT a.phone FROM `airent_tbl_user_many_identi_info` a  WHERE a.user_id='" + str(
        userid) + "') a"
    fcursor.execute(sql)
    (aliphone,) = fcursor.fetchone()

    # 用户转化为老用户的时间:签收日期+8 改为签收时间作为用户转化时间
    # sql = "select date(date_add(min(a.dt_signed),INTERVAL 8 DAY)) as dt_transform " \
    #       "from ods.airent_e_trade a " \
    #       " where a.dt_signed is not null and date(a.dt_signed)< date(DATE_SUB(now(),INTERVAL 7 DAY)) " \
    #       "and a.status in (10,26,27,28,30) and id_user='"+ str(userid)+"'"
    sql="select min(a.dt_signed) as dt_transform " \
        "from( " \
        "select a.id_user,ifnull(a.dt_signed,'2099-12-31 23:59:59') as dt_signed " \
        "from ods.airent_e_trade a " \
        "where a.dt_signed is not null and a.status in (10,26,27,28,30) and a.id_user='"+str(userid)+"'" +\
        "union all select a.id_user,ifnull(a.dt_signed,'2099-12-31 23:59:59') as dt_signed " \
        "from( " \
        "select a.id_user,a.id,a.dt_signed, " \
        "ifnull(max(b.dt_created),ADDDATE(now(),interval 10 day)) as return_time," \
        "ifnull(max(c.dt_created),ADDDATE(now(),interval 10 day)) as operate_time," \
        "least(ifnull(max(b.dt_created),ADDDATE(now(),INTERVAL 10 day)),ifnull(max(c.dt_created),ADDDATE(now(),INTERVAL 10 day))) as back_time " \
        "from ods.airent_e_trade a " \
        "left join ods.airent_tbl_trade_log b on a.id = b.id_trade and b.new_status = 8  " \
        "left join ods.airent_tbl_trade_log c on a.id = c.id_trade and c.new_status =15 " \
        "where a.dt_signed is not null and a.status in (8,15) and a.id_user ='"+str(userid)+"'" +\
        "group by a.id) a " \
        "where a.back_time > ADDDATE(a.dt_signed,interval 7 day)" \
        ")a group by a.id_user "
    fcursor.execute(sql)
    (dt_transform,)=fcursor.fetchone()

    return (id_user,dt_created,dt_transform,utm_source,utm_medium,utm_campaign,is_active,name,identi_card,applyOrds,payedOrds,onLineOrds,overdueOrds,sendphone,regphone,aliphone,buyoutOrds,returnOrds,returnedOrds,reorderOrds)

def insertUserInfo(toconn,tcursor,result):
    sql = "insert into user_detail(id_user,dt_created,dt_transform,utm_source,utm_medium,utm_campaign,is_active,name,identi_card,applyOrds,payedOrds,onLineOrds,overdueOrds,sendphone,regphone,aliphone,buyoutOrds,returnOrds,returnedOrds,reorderOrds) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    tcursor.execute(sql, result)
    toconn.commit()
def delete(toconn,tcursor,userid):
    sql = "delete from user_detail where id_user='"+userid+"'"
    tcursor.execute(sql)
    toconn.commit()

if __name__ == "__main__":
    sql = """
    SELECT DISTINCT id_user 
    FROM (
        SELECT   b.id_user 
          FROM ods.airent_tbl_trade_log a 
        LEFT JOIN ods.airent_e_trade b ON a.id_trade=b.id
        WHERE a.new_status IN (8,27,28,30,15) 
        AND a.dt_created between concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 00:00:00') and concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 23:59:59')
        UNION ALL
        SELECT  user_id FROM `airent_tbl_user_utm_info` WHERE DATE(dt_created)=SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10) 
        UNION ALL
        SELECT b.id_user FROM airent_e_sub_trade a LEFT JOIN airent_e_trade b ON a.main_trade_no=b.trade_no WHERE a.main_trade_no<>a.sub_trade_no AND a.sub_trade_type=1 AND a.status=2 AND a.dt_created between concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 00:00:00') and concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 23:59:59')
        UNION ALL
        SELECT id_user FROM airent_e_trade WHERE dt_created between concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 00:00:00') and concat(SUBSTR(DATE_ADD(NOW(), INTERVAL -1 DAY),1,10),' 23:59:59')
        union all
        select b.id_user
        from ods.airent_tbl_trade_log a
        left join ods.airent_e_trade b on a.id_trade = b.id
        where a.dt_created > date_sub(now(),INTERVAL 16 day) and a.new_status = 10
) a
    """
    fromcursor.execute(sql)
    res=fromcursor.fetchall()
    for re in res:
        userid = str(re[0])
        try:
            if chargeUserIn(tocursor, userid):
                delete(toconn, tocursor, userid)
                insertUserInfo(toconn, tocursor, getresult(fromcursor, userid))
            else:
                insertUserInfo(toconn, tocursor, getresult(fromcursor, userid))
        except Exception,e:
            print e



#断开数据库链接
fromconn.close()
toconn.close()