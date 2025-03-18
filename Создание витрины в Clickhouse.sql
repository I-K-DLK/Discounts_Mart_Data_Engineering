CREATE TABLE discounts.test_view(
id Int32,
num Int32
)
ENGINE =  PostgreSQL('192.168.214.203:5432','adb','test_view','user','pass','discounts');

SELECT * FROM discounts.test_view;

DROP TABLE discounts.test_view;

CREATE TABLE discounts.test_mart_from_view ON CLUSTER default_cluster (
id Int32,
num Int32
)
ENGINE = ReplicatedMergeTree('/click/test_mart_from_view/{shard}','{replica}')
ORDER BY id

CREATE materialized VIEW discounts.test_mv ON CLUSTER default_cluster 
ENGINE = ReplicatedMergeTree('/click/test_materialized_view/{shard}','{replica}')
ORDER BY id
AS SELECT * FROM discounts.test_view

--DROP discounts.test_mv

SELECT * FROM discounts.test_mv;

SELECT * FROM discounts.test_mart_from_view


/*
1. Создадим базу данных discounts на 206 хосте.
 */


CREATE DATABASE IF NOT EXISTS discounts;

 
--2. Создадим внешнюю таблицу для витрины данных из Greenplum откуда будут считываться данные
 
-- DROP TABLE discounts.gp_sales_traffic_mart

CREATE TABLE IF NOT EXISTS discounts.gp_sales_traffic_mart
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
) ENGINE =  PostgreSQL('192.168.214.203:5432','adb','ch_sales_traffic_mart','user','pass','discounts');
 
-- Создадим реплицированные таблицы витрины данных 

-- DROP TABLE discounts.ch_sales_traffic_mart_daily;

CREATE TABLE discounts.ch_sales_traffic_mart ON CLUSTER default_cluster 
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
ORDER BY ("Дата","Код завода");

-- Создалим обновляемое представление для загрузки данных из внешней таблицы в реплицированную

CREATE MATERIALIZED VIEW discounts.sales_traffic_view ON CLUSTER default_cluster 
REFRESH EVERY 1 MINUTE
TO discounts.ch_sales_traffic_mart
AS SELECT * FROM discounts.gp_sales_traffic_mart

 


 
