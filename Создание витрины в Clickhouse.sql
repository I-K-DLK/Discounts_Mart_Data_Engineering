 
-- Создадим базу данных discounts на 206 хосте.
  
CREATE DATABASE IF NOT EXISTS discounts;

 
-- Создадим внешнюю таблицу для витрины данных из Greenplum откуда будут считываться данные
 
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

 


 
