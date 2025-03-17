  
-- Создадим рабочую схему discounts (dcs)

CREATE schema dcs;

-- Создадим таблицы из внешних источников

--  bills_head, bills_item (таблицы с данными из чеков) из загрузим из БД PostgreSQL через pxf
 
DROP EXTERNAL TABLE IF EXISTS dcs.bills_head_ext;

CREATE READABLE EXTERNAL TABLE dcs.bills_head_ext (
	billnum bigint,
	plant text,
	calday date)

LOCATION ('pxf://gp.bills_head?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=user&PASS=pass')
ON ALL 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
  
DROP EXTERNAL TABLE IF EXISTS dcs.bills_item_ext; 

CREATE READABLE EXTERNAL TABLE dcs.bills_item_ext (
	billnum bigint,
	billitem bigint,
	material bigint,
	qty int,
	netval numeric(17, 2),
	tax numeric(17, 2),
	rpa_sat numeric(17, 2),
	calday date)

LOCATION ('pxf://gp.bills_item?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=user&PASS=pass')
ON ALL 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


-- traffic (таблица посещаемости магазинов сети)

DROP EXTERNAL TABLE IF EXISTS dcs.traffic_ext;

CREATE READABLE EXTERNAL TABLE dcs.traffic_ext (
	plant text,
	date text,
	time text,
	frame_id text,
	quantity int
	)

LOCATION ('pxf://gp.traffic?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=user&PASS=pass')
ON ALL 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

SELECT * FROM dcs.bills_head_ext LIMIT 10;

/*
billnum  |billitem|material  |qty|netval |tax   |rpa_sat|calday    |
---------+--------+----------+---+-------+------+-------+----------+
900979502|       2|1000031832|  3| 220.00| 44.00| 264.00|2021-02-07|
901221149|       2|     20404|  1|3416.67|683.33|4100.00|2021-02-24|
906250606|      10|     55370|  1| 237.27| 23.73| 261.00|2021-01-05|
906381516|       5|2037787001|  1|   0.83|  0.17|   1.00|2021-01-07|
906531909|      10|     55205|  2| 576.67|115.33| 692.00|2021-01-14|
906651432|       3| 108123969|  1| 815.83|163.17| 979.00|2021-01-16|
907439035|       3|2039296003|  1|  32.50|  6.00|  39.00|2021-02-22|
900439071|       3|     20704|  3| 250.00| 50.00| 300.00|2021-02-03|
900551332|       8|2028062004|  1|  73.33| 14.67|  88.00|2021-01-04|
901154502|       2|1000041426|  1|   1.82|  0.18|   2.00|2021-02-21|
*/

SELECT * FROM dcs.bills_item_ext LIMIT 10;
/*
billnum  |billitem|material  |qty|netval |tax   |rpa_sat|calday    |
---------+--------+----------+---+-------+------+-------+----------+
900979502|       2|1000031832|  3| 220.00| 44.00| 264.00|2021-02-07|
901221149|       2|     20404|  1|3416.67|683.33|4100.00|2021-02-24|
906250606|      10|     55370|  1| 237.27| 23.73| 261.00|2021-01-05|
906381516|       5|2037787001|  1|   0.83|  0.17|   1.00|2021-01-07|
906531909|      10|     55205|  2| 576.67|115.33| 692.00|2021-01-14|
906651432|       3| 108123969|  1| 815.83|163.17| 979.00|2021-01-16|
907439035|       3|2039296003|  1|  32.50|  6.00|  39.00|2021-02-22|
900439071|       3|     20704|  3| 250.00| 50.00| 300.00|2021-02-03|
900551332|       8|2028062004|  1|  73.33| 14.67|  88.00|2021-01-04|
901154502|       2|1000041426|  1|   1.82|  0.18|   2.00|2021-02-21|
*/

SELECT * FROM dcs.traffic_ext LIMIT 10;

/*
plant|date      |time  |frame_id  |quantity|
-----+----------+------+----------+--------+
M001 |01.01.2021|080000|1245776664|       2|
M001 |01.01.2021|090000|1245776667|       2|
M001 |01.01.2021|100000|1245776670|       1|
M001 |01.01.2021|110000|1245776673|       0|
M001 |01.01.2021|120000|1245776676|       0|
M001 |01.01.2021|130000|1245776679|       1|
M001 |01.01.2021|140000|1245776692|       1|
M001 |01.01.2021|150000|1245776695|       1|
M001 |01.01.2021|160000|1245776698|       1|
M001 |01.01.2021|170000|1245776691|       1|
*/

-- Данные из других источников загрузим по протоколу gpfdist из локальных файлов CSV

-- Скидочные купоны
DROP EXTERNAL TABLE IF EXISTS dcs.coupons_ext;
 
CREATE EXTERNAL TABLE dcs.coupons_ext(plant varchar, calday date, coupon_num varchar,  coupon_promo text, material bigint, billnum bigint
)
LOCATION('gpfdist://172.16.128.98:8080/coupons.csv')
FORMAT 'CSV'(HEADER DELIMITER ',' NULL '');

 -- Типы промо-акций
DROP EXTERNAL TABLE IF EXISTS dcs.promo_types_ext;

CREATE EXTERNAL TABLE dcs.promo_types_ext(promo_type int, txt text
)
LOCATION('gpfdist://172.16.128.98:8080/promo_types.csv')
FORMAT 'CSV'(HEADER DELIMITER ',' NULL '');

 -- Промо-акции 
DROP EXTERNAL TABLE IF EXISTS dcs.promos_ext;

CREATE EXTERNAL TABLE dcs.promos_ext(id text, title text, promo_type int, material bigint, discount int
)
LOCATION('gpfdist://172.16.128.98:8080/promos.csv')
FORMAT 'CSV'(HEADER DELIMITER ',' NULL '');

 -- Магазины
DROP EXTERNAL TABLE IF EXISTS stores_ext;

CREATE EXTERNAL TABLE stores_ext(plant varchar, txt text
)
LOCATION('gpfdist://172.16.128.98:8080/stores.csv')
FORMAT 'CSV'(HEADER DELIMITER ',' NULL '');

SELECT * FROM dcs.stores_ext;

/*
plant|txt        |
-----+-----------+
M001 |Магазин №1 |
M002 |Магазин №2 |
M003 |Магазин №3 |
M004 |Магазин №4 |
M005 |Магазин №5 |
M006 |Магазин №6 |
M007 |Магазин №7 |
M008 |Магазин №8 |
M009 |Магазин №9 |
M010 |Магазин №10|
M011 |Магазин №11|
M012 |Магазин №12|
M013 |Магазин №13|
M014 |Магазин №14|
M015 |Магазин №15|
*/

SELECT * FROM dcs.promo_types_ext;
/*
promo_type|txt                             |
----------+--------------------------------+
         1|Скидка в абсолютном выражении   |
         2|Скидка в относительном выражении|
         */
SELECT * FROM std937.promos_ext;

/*
plant|calday    |coupon_num|coupon_promo                    |material  |billnum  |
-----+----------+----------+--------------------------------+----------+---------+
M003 |2021-01-01|A000001   |3638616237621EEBA4EF73792BE10EAE|     32204|900443454|
M003 |2021-01-01|A000002   |3638616237621ECJIO7R77DF8B364EB8|     55414|900443454|
M003 |2021-01-01|A000003   |3638616237621EEBA4EF75D65ADA0EB1|     55417|900443454|
M003 |2021-01-01|A000004   |3638616237621EEBA4EF77DF8B364EB8|     70245|900443454|
M005 |2021-01-01|A000005   |005056A75DDC1EEAA9BC2CA7D9AA66EE|7000009745|900498502|
M013 |2021-01-03|A000006   |005056A75DDC1EEAA9BC2CA7D9AA66EE|7000009745|900461404|
M007 |2021-01-03|A000007   |3638616237621EEBA4EF73792BE10EAE|     32204|900796341|
M003 |2021-01-04|A000008   |005056A75DDC1EEAA9BC2CA7D9AA66EE|7000009745|900541857|
M003 |2021-01-04|A000009   |005056A75DDC1EEAA9BC2CA7D9AA66EE|7000009745|900541857|
M012 |2021-01-04|A000010   |3638616237621EEBA4EF73792BE10EAE|     32204|900658343|
*/

/*
Создадим локальные таблицы в Greenplum на основе внешних таблиц. Выберем распределение и партицирование.
Для OLAP нагрузки везде используем тип appendoptimized и колоночную ориентацию.
Степень сжатия выбираем 1 для оптимального быстродействия.
*/
	
-- Создадим справочники - реплицируем их на все сегменты. 

 
DROP TABLE IF EXISTS dcs.promo_types;

CREATE TABLE dcs.promo_types (
	promo_type int,
	txt text
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS dcs.promos;

CREATE TABLE dcs.promos (
	id text,
	title text,
	promo_type int,
	material bigint,
	discount int	
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED REPLICATED;
 

DROP TABLE IF EXISTS dcs.stores;

CREATE TABLE dcs.stores (
	plant varchar,
	txt text
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED REPLICATED;


-- Создадим таблицы фактов - партиционирование установим по колонкам с типом данных - дата, помесячно

select count(*) from bills_head_ext -- 9967126 записей

 
-- DROP TABLE dcs.traffic;

CREATE TABLE dcs.traffic (plant text, date date, time text, frame_id text, quantity int
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(date)
(
PARTITION ym START ('2020-01-01') END ('2026-01-01') EVERY ( INTERVAL '1 month'),
DEFAULT PARTITION other
);
-- DROP TABLE dcs.bills_head;

CREATE TABLE dcs.bills_head (LIKE bills_head_ext
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE(calday)
(
PARTITION ym START ('2020-01-01') END ('2026-01-01') EVERY ( INTERVAL '1 month'),
DEFAULT PARTITION other
);


-- DROP TABLE dcs.bills_item;

CREATE TABLE dcs.bills_item (LIKE bills_item_ext
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(calday)
(
PARTITION ym START ('2020-01-01') END ('2026-01-01') EVERY ( INTERVAL '1 month'),
DEFAULT PARTITION other
);

-- DROP TABLE dcs.coupons;

CREATE TABLE dcs.coupons (
	plant varchar,
	calday date,
	coupon_num varchar,
	coupon_promo text,
	material bigint,
	billnum bigint
	
)
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
DISTRIBUTED BY(coupon_num)
PARTITION BY RANGE(calday)
(
PARTITION ym START ('2020-01-01') END ('2026-01-01') EVERY ( INTERVAL '1 month'),
DEFAULT PARTITION other
);
  
