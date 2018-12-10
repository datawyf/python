#测试
select a.id_user,min(a.dt_signed) as dt_signed
from(
		select a.id_user,ifnull(a.dt_signed,'2099-12-31 23:59:59') as dt_signed
		from ods.airent_e_trade a
		where a.dt_signed is not null and a.status in (10,26,27,28,30)  -- and a.id_user = '13619015'
		union all
		select a.id_user,ifnull(a.dt_signed,'2099-12-31 23:59:59') as dt_signed
		from(
		select a.id_user,a.id,a.dt_signed,
		ifnull(max(b.dt_created),ADDDATE(now(),interval 10 day)) as return_time,
		ifnull(max(c.dt_created),ADDDATE(now(),interval 10 day)) as operate_time,
		least(ifnull(max(b.dt_created),ADDDATE(now(),INTERVAL 10 day)),ifnull(max(c.dt_created),ADDDATE(now(),INTERVAL 10 day))) as back_time
		from ods.airent_e_trade a
		left join ods.airent_tbl_trade_log b on a.id = b.id_trade and b.new_status = 8
		left join ods.airent_tbl_trade_log c on a.id = c.id_trade and c.new_status =15
		where a.dt_signed is not null and a.status in (8,15)  -- and a.id_user = '1041854'
		group by a.id
		) a
		where a.back_time > ADDDATE(a.dt_signed,interval 7 day)
)a
group by a.id_user
;


select a.id,a.id_user,a.dt_signed,a.trade_no,a.status,a.*
from ods.airent_e_trade a
where a.dt_signed is not null and a.status in (10,26,27,28,30,8,15)  and a.id_user = '1256302'
order by a.dt_signed desc;


select *
from ods.airent_tbl_trade_log
where id_trade = '326979';


select date(date_add(min(a.dt_signed),INTERVAL 8 DAY)) as dt_transform
from ods.airent_e_trade a
 where a.dt_signed is not null and date(a.dt_signed)< date(DATE_SUB(now(),INTERVAL 7 DAY))
 and a.status in (10,26,27,28,30) and id_user='1256302';


 select a.*
 from dw.user_detail a
 left join dw.user_detail_20181210 b  on a.id_user = b.id_user
 where a.aliphone<> b.aliphone
 or a.applyOrds <> b.applyOrds
 or a.buyoutOrds<> b.buyoutOrds
 or a.dt_created<> b.dt_created
 or a.identi_card<> b.identi_card
 or a.is_active<> b.is_active
 or a.`name`<> b.`name`
 or a.onLineOrds<> b.onLineOrds
 or a.overdueOrds<> b.overdueOrds
 or a.payedOrds <> b.payedOrds
 or a.regphone <> b.regphone
 or a.reorderOrds <> b.reorderOrds
 or a.returnOrds <> b.returnOrds
 or a.returnedOrds <> b.returnedOrds
 or a.sendphone <> b.sendphone
 or a.utm_campaign <> b.utm_campaign
 or a.utm_medium <> b.utm_medium
 or a.utm_source <> b.utm_source
 ;
 select count(1)
 from dw.user_detail;

 select count(1)
 from dw.user_detail_20181210


select *
from dw.user_detail_update a
where a.dt_transform_date is null and a.dt_transform is not null and date(a.dt_transform)<'2018-12-03'
order by a.dt_transform desc;348
;

select *
from dw.user_detail_update a
where date(a.dt_transform) <> DATE_ADD(a.dt_transform_date,INTERVAL -8 day);45

## 注意'2099-12-31 23:59:59'

## 创建临时表
create table wyf_user_detail_return
-- explain
		select a.id_user,a.id,a.dt_signed,ifnull(max(b.dt_created),ADDDATE(now(),interval 10 day)) as return_time,ifnull(max(c.dt_created),ADDDATE(now(),interval 10 day)) as operate_time,
		 least(ifnull(max(b.dt_created),ADDDATE(now(),INTERVAL 10 day)),ifnull(max(c.dt_created),ADDDATE(now(),INTERVAL 10 day))) as back_time
		from ods.airent_e_trade a
		left join ods.airent_tbl_trade_log b on a.id = b.id_trade and b.new_status = 8
		left join ods.airent_tbl_trade_log c on a.id = c.id_trade and c.new_status =15
		where a.dt_signed is not null and a.status in (8,15) -- and a.id_user = '11076155'
		group by a.id
;

create table wyf_user_detail_transform
select a.id_user,min(a.dt_signed) as dt_transform
from(
		select a.id_user,a.dt_signed
		from ods.airent_e_trade a
		where a.dt_signed is not null and a.status in (10,26,27,28,30) -- and a.id_user = '11076155'
		union all
		select a.id_user,a.dt_signed
		from wyf_user_detail_return a
		where a.back_time > ADDDATE(a.dt_signed,interval 7 day)
)a
group by a.id_user
;

## id_user更新数据
select a.id_user,min(a.dt_signed)
from(
		select a.id_user,a.dt_signed
		from ods.airent_e_trade a
		where a.dt_signed is not null and a.status in (10,26,27,28,30) -- and a.id_user = '11076155'
		union all
		select a.id_user,a.dt_signed
		from(
		select a.id_user,a.id,a.dt_signed,ifnull(max(b.dt_created),ADDDATE(now(),interval 10 day)) as return_time,ifnull(max(c.dt_created),ADDDATE(now(),interval 10 day)) as operate_time,
		 least(ifnull(max(b.dt_created),ADDDATE(now(),INTERVAL 10 day)),ifnull(max(c.dt_created),ADDDATE(now(),INTERVAL 10 day))) as back_time
		from ods.airent_e_trade a
		left join ods.airent_tbl_trade_log b on a.id = b.id_trade and b.new_status = 8
		left join ods.airent_tbl_trade_log c on a.id = c.id_trade and c.new_status =15
		where a.dt_signed is not null and a.status in (8,15) -- and a.id_user = '11076155'
		group by a.id
		) a
		where a.back_time > ADDDATE(a.dt_signed,interval 7 day)
)a
group by a.id_user
;


