CREATE TABLE std937.test_view(
id Int32,
num Int32
)
ENGINE =  PostgreSQL('192.168.214.203:5432','adb','test_view','std9_37','e9XhF9F0BHAgDO','std937');

SELECT * FROM std937.test_view;

DROP TABLE std937.test_view;

CREATE TABLE std937.test_mart_from_view ON CLUSTER default_cluster (
id Int32,
num Int32
)
ENGINE = ReplicatedMergeTree('/click/test_mart_from_view/{shard}','{replica}')
ORDER BY id

CREATE materialized VIEW std937.test_mv ON CLUSTER default_cluster 
ENGINE = ReplicatedMergeTree('/click/test_materialized_view/{shard}','{replica}')
ORDER BY id
AS SELECT * FROM std937.test_view

--DROP std937.test_mv

SELECT * FROM std937.test_mv;

SELECT * FROM std937.test_mart_from_view

CREATE MATERIALIZED VIEW std937.test_mv_rf  
--REFRESH EVERY 1 MINUTE TO std937.test_mart_from_view
ENGINE =  MergeTree()
ORDER BY id
AS SELECT * FROM std937.test_view

/*
1. Создайте базу данных std<номер пользователя> на 206 хосте.
 */


CREATE DATABASE IF NOT EXISTS std937;

 
--2. Создадим внешние таблицы для витрин данных из Greenplum
 
--DROP TABLE std937.ch_sales_traffic_mart

CREATE TABLE IF NOT EXISTS std937.ch_sales_traffic_mart
(   
	"Дата" Date,
    "Код завода" String,
	"Завод" String,
	"Оборот" Decimal(17,6),
	"Скидки по купонам" Decimal(17,6),
	"Оборот с учетом скидки" Decimal(17,6),
	"Кол-во проданных товаров" Int32,
	"Количество чеков" Int32,
	"Трафик" Decimal(17,6),
	"Кол-во товаров по акции" Int8,
	"Доля товаров со скидкой" String,
	"Среднее количество товаров в чеке" Decimal(17,6),
	"Коэффициент конверсии магазина, %" String,
	"Средний чек, руб." Decimal(17,6),
	"Средняя выручка на одного посетителя"  Decimal(17,6) 
) ENGINE =  PostgreSQL('192.168.214.203:5432','adb','ch_sales_traffic_mart','std9_37','e9XhF9F0BHAgDO','std937');

SELECT * FROM std937.ch_sales_traffic_mart
 
-- Создадим реплицированные таблицы витрины данных 

-- DROP TABLE std937.ch_sales_traffic_mart_daily;

CREATE TABLE std937.ch_sales_traffic_mart_copy ON CLUSTER default_cluster 
(
	"Дата" Date,
    "Код завода" String,
	"Завод" String,
	"Оборот" Decimal(17,6),
	"Скидки по купонам" Decimal(17,6),
	"Оборот с учетом скидки" Decimal(17,6),
	"Кол-во проданных товаров" Int32,
	"Количество чеков" Int32,
	"Трафик" Decimal(17,6),
	"Кол-во товаров по акции" Int8,
	"Доля товаров со скидкой" String,
	"Среднее количество товаров в чеке" Decimal(17,6),
	"Коэффициент конверсии магазина, %" String,
	"Средний чек, руб." Decimal(17,6),
	"Средняя выручка на одного посетителя"  Decimal(17,6)
)  
ENGINE = ReplicatedMergeTree('/click/ch_sales_traffic_mart/{shard}','{replica}')
PARTITION BY toYYYYMM("Дата")
ORDER BY ("Дата","Код завода");


SELECT * FROM std937.ch_sales_traffic_mart_copy

DROP std937.ch_sales_traffic_mart_copy
CREATE TABLE std937.ch_sales_traffic_mart_distr AS std937.ch_sales_traffic_mart
ENGINE = Distributed('default_cluster','std937','ch_sales_traffic_mart',RIGHT("Код завода",1)::int)


SELECT * FROM std937.ch_sales_traffic_mart_distr


---------------
--Superset Query 


 with bills_cte as(

select bh.billnum as billnum, bi.billitem as billitem, bi.material as material, bi.qty as qty, netval, bi.tax as tax, bi.rpa_sat as rpa_sat,  bh.calday as calday, s.plant as plant, s.txt as txt 
from std937.ch_bills_item_ext bi 
	join std937.ch_bills_head_ext bh on bh.billnum = bi.billnum
	left join std937.ch_stores_ext s on bh.plant = s.plant	 
where --bh.calday >= '2021-01-01' and bh.calday <= '2021-02-28'
    1 = 1 and (
    {% if from_dttm is not none %}
        bh.calday >= date('{{ from_dttm }}') and
    {% endif %}
    {% if to_dttm is not none %}
        bh.calday <= date('{{ to_dttm }}') and
    {% endif %}
    true
)
),
coupon_cte as(

select  c.coupon_num as coupon_num, bc.plant as plant,
case when p.promo_type = 1 then p.discount 
	 when p.promo_type = 2 then (bc.rpa_sat / bc.qty * ( p.discount) / 100) 
	 else null end as discount,
rank() over(partition by c.coupon_num order by bc.billitem) as rank
from bills_cte bc 
	 join std937.ch_coupons_ext  c on c.material = bc.material and c.billnum = bc.billnum and c.calday = bc.calday
	 join std937.ch_promos_ext  p on p.id = c.coupon_promo and p.material = c.material    
),

coupons_agg as (

select cc.plant as plant, count(distinct cc.coupon_num) as discounted_goods, round(sum(case when rank = 1 then cc.discount else null end),2) as discounts   
from coupon_cte cc  
group by cc.plant
),

sales_agg as (

select bc.plant as plant, bc.txt as txt, round(sum(bc.rpa_sat),0) as turnover, sum(bc.qty) as total_sales, count(distinct (bc.plant, bc.billnum, bc.calday)) as total_bills 
from bills_cte bc
group by bc.plant, bc.txt
),

traffic_agg as (

select t.plant as plant, sum(t.quantity)::decimal(17,2) as traffic
from std937.ch_traffic_ext  t 
where --t.date >= '2021-01-01' and t.date <= '2021-02-28')
    1=1 and (
    {% if from_dttm is not none %}
        t.date >= date('{{ from_dttm }}') AND
    {% endif %}
    {% if to_dttm is not none %}
        t.date <= date('{{ to_dttm }}') AND
    {% endif %}
    true
)
group by t.plant 
)

select ta.plant as "Код завода",sa.txt as "Завод", sa.turnover "Оборот", ca.discounts as "Скидки по купонам", sa.turnover - ca.discounts as "Оборот с учетом скидки",
sa.total_sales as "Кол-во проданных товаров",sa.total_bills as "Количество чеков", ta.traffic as "Трафик", ca.discounted_goods as "Кол-во товаров по акции",  
round(ca.discounted_goods/sa.total_sales *100,1) as "Доля товаров со скидкой,%", round(sa.total_sales::decimal(17,6)/sa.total_bills,2) as "Среднее количество товаров в чеке",
round((sa.total_bills::decimal(17,6)/ta.traffic*100),2) as "Коэффициент конверсии магазина, %", round(sa.turnover/sa.total_bills,1) as "Средний чек, руб.", 
round(sa.turnover/ta.traffic,1) as "Средняя выручка на одного посетителя, руб" 

from coupons_agg ca 
	join sales_agg sa on sales_agg.plant = coupons_agg.plant 
	join traffic_agg ta on coupons_agg.plant = traffic_agg.plant
order by ta.plant 


--------------------------------


/*
with bills_cte as(

select bh.billnum as billnum, bi.billitem as billitem, bi.material as material, bi.qty as qty, netval, bi.tax as tax, bi.rpa_sat as rpa_sat,  bh.calday as calday, s.plant as plant, s.txt as txt 
from std937.ch_bills_item_ext bi 
	join std937.ch_bills_head_ext bh on bh.billnum = bi.billnum
	left join std937.ch_stores_dict s on bh.plant = s.plant	 
where bh.calday between '2021.01.01' and '2021.02.28'),

coupon_cte as(

select  c.coupon_num as coupon_num, bc.plant as plant,
case when p.promo_type = 1 then p.discount 
	 when p.promo_type = 2 then (bc.rpa_sat/bc.qty * ( p.discount)/100) 
	 else null end as discount,
rank() over(partition by c.coupon_num order by bc.billitem) as rank
from bills_cte bc 
	left join std937.ch_coupons_dict  c on (c.material = bc.material and c.billnum = bc.billnum and c.calday = bc.calday) 
	left join std937.ch_promos_dict  p on p.id = c.coupon_promo and p.material = c.material    
),

coupons_agg as (

select cc.plant as plant, count(distinct cc.coupon_num) as discounted_goods, round(sum(case when rank = 1 then cc.discount else null end),2) as discounts   
from coupon_cte cc  
group by cc.plant
order by cc.plant),

sales_agg as (

select bc.plant as plant, bc.txt as txt, round(sum(bc.rpa_sat),0) as turnover, sum(bc.qty) as total_sales, count(distinct (bc.plant, bc.billnum, bc.calday)) as total_bills 
from bills_cte bc
group by bc.plant,bc.txt
order by bc.plant),

traffic_agg as (

select t.plant as plant, sum(t.quantity)::decimal as traffic
from std937.ch_traffic_ext  t 
where t."date" between '2021.01.01' and '2021.02.28' 
group by t.plant 
order by t.plant 
)

select ta.plant as "Завод",sa.txt as "Завод", sa.turnover "Оборот", ca.discounts as "Скидки по купонам", sa.turnover - ca.discounts as "Оборот с учетом скидки",
sa.total_sales as "Кол-во проданных товаров",sa.total_bills as "Количество чеков", ta.traffic as "Трафик", ca.discounted_goods as "Кол-во товаров по акции",  
concat(round(ca.discounted_goods/sa.total_sales::decimal*100,1)::text,'%') as "Доля товаров со скидкой", round(sa.total_sales::decimal/sa.total_bills,2) as "Среднее количество товаров в чеке",
concat(round(sa.total_bills/ta.traffic*100,2)::text,'%') as "Коэффициент конверсии магазина, %", round(sa.turnover/sa.total_bills,1) as "Средний чек, руб.", 
round(sa.turnover/ta.traffic,1) as "Средняя выручка на одного посетителя, руб" 

from coupons_agg ca 
	join sales_agg sa using(plant) 
	join traffic_agg ta using(plant)
order by plant;
 * */


DROP DICTIONARY IF EXISTS std937.ch_promo_types_dict

CREATE DICTIONARY std937.ch_promo_types_dict
(
	promo_type UInt8,
	txt String
)
PRIMARY KEY promo_type
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'std9_37'
    password 'e9XhF9F0BHAgDO'
    db 'adb'
    query 'SELECT promo_type, txt FROM std937.promo_types'
     
))
LAYOUT(FLAT())
LIFETIME(300)

SELECT * FROM std937.ch_promo_types_dict



DROP DICTIONARY IF EXISTS std937.ch_promos_dict

CREATE DICTIONARY std937.ch_promos_dict
(
	id String,
	title String,
	promo_type UInt32,
	material UInt64,
	discount UInt32
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'std9_37'
    password 'e9XhF9F0BHAgDO'
    db 'adb'
    query 'SELECT id, title,promo_type,material,discount FROM std937.promos'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)

SELECT * FROM std937.ch_promos_dict


DROP DICTIONARY IF EXISTS std937.ch_stores_dict

CREATE DICTIONARY std937.ch_stores_dict
(
	plant String,
	txt String
)
PRIMARY KEY plant
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'std9_37'
    password 'e9XhF9F0BHAgDO'
    db 'adb'
    query 'SELECT  plant,  txt FROM std937.stores'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)

SELECT * FROM std937.ch_stores_dict


DROP DICTIONARY IF EXISTS std937.ch_coupons_dict

CREATE DICTIONARY std937.ch_coupons_dict
(
	plant String,
	coupon_num String,
	calday Date,
	coupon_promo String,
	material UInt64,
	billnum UInt64
)
PRIMARY KEY coupon_num
SOURCE(POSTGRESQL(
    port 5432
    host '192.168.214.203'
    user 'std9_37'
    password 'e9XhF9F0BHAgDO'
    db 'adb'
    query 'SELECT coupon_num, plant, calday, coupon_promo, material, billnum FROM std937.coupons'
))
LAYOUT(COMPLEX_KEY_HASHED)
LIFETIME(300)

SELECT coupon_num, plant, calday, coupon_promo, material, billnum FROM std937.ch_coupons_dict

 
 
SELECT * FROM std937.test_ext
CREATE TABLE std937.test_ext (`id` Int32, `text1` String, `text2` String) ENGINE = MergeTree ORDER BY id
INSERT INTO std937.test_ext VALUES(23,'rfge','ethe');


CREATE TABLE std937.test (LIKE  std937.tkhemali_test_ext) ENGINE JDBC('jdbc:clickhouse://192.168.214.206:8123?user=std9_37&password=e9XhF9F0BHAgDO', 'std937', 'test')

DROP TABLE text_jdbc
CREATE TABLE std937.text_jdbc (
     `id` Int32, `text1` String, `text2` String 
      
)
ENGINE = JDBC('mysql8', 'mydatabase', 'mytable');

SELECT * FROM mytable;