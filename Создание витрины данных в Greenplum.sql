/*
Для тестирования правильности сборки витрины данных построим отчет за период 01.01.2021-28.02.2021 и сверим результат с эталоном из Excel файла (Итоговый отчет).
Если отчет за 2 месяца будет выполнен верно, сменим периодичность формирования витрины на ежедневное, для возможности получения актуальных данных аналитиками.
Итоговый отчёт представлен в файле во вложении. В примере отчет собран при помощи стандартных формул excel. Исходные данные для формирования отчета содержатся на других листах.
*/


-- Составим запрос на формирование витрины, проверим результат для тестовых дат

with bills_cte as (

select bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
from dcs.bills_item bi 
	join dcs.bills_head bh on bh.billnum = bi.billnum
	left join dcs.stores s on bh.plant = s.plant	 
where (bh.calday >= '2021.01.01'  and bh.calday <= '2021.02.28') 
	and (bi.calday >= '2021.01.01' and bi.calday <= '2021.02.28')),  

traffic_agg as (
 
select t.plant, sum(t.quantity)::decimal as traffic
from dcs.traffic t 
where t."date" >= '2021.01.01' and t."date"<= '2021.02.28' 
group by t.plant  
), 

coupon_cte as(

select  c.coupon_num, bc.plant,
case when p.promo_type = 1 then p.discount 
	 when p.promo_type = 2 then (bc.rpa_sat/bc.qty * ( p.discount)/100) 
	 else null end as discount,
rank() over(partition by c.coupon_num order by bc.billitem) as rank
from bills_cte bc 
	left join dcs.coupons c on c.billnum = bc.billnum AND c.material = bc.material
	left join dcs.promos p on p.id = c.coupon_promo and p.material = c.material
where c.calday >= '2021.01.01' and c.calday <= '2021.02.28' 
),

coupons_agg as (

select cc.plant, count(distinct cc.coupon_num) as discounted_goods, round(sum(case when rank = 1 then cc.discount else null end),2) as discounts   
from coupon_cte cc  
group by cc.plant
),

sales_agg as (

select bc.plant,bc.txt, round(sum(bc.rpa_sat),0) as turnover, sum(bc.qty) as total_sales, count(distinct bc.billnum) as total_bills 
from bills_cte bc
group by bc.plant, bc.txt
) 
 
select ta.plant as "Завод",sa.txt as "Завод", sa.turnover "Оборот", ca.discounts as "Скидки по купонам", sa.turnover - ca.discounts as "Оборот с учетом скидки",
sa.total_sales as "Кол-во проданных товаров",sa.total_bills as "Количество чеков", ta.traffic as "Трафик", ca.discounted_goods as "Кол-во товаров по акции",  
concat(round(ca.discounted_goods/sa.total_sales::decimal*100,1)::text,'%') as "Доля товаров со скидкой", round(sa.total_sales::decimal/sa.total_bills,2) as "Среднее количество товаров в чеке",
concat(round(sa.total_bills/ta.traffic*100,2)::text,'%') as "Коэффициент конверсии магазина, %", round(sa.turnover/sa.total_bills,1) as "Средний чек, руб.", 
round(sa.turnover/ta.traffic,1) as "Средняя выручка на одного посетителя, руб" 

from coupons_agg ca 
	join sales_agg sa on ca.plant = sa.plant
	join traffic_agg ta on ta.plant = sa.plant
order by ta.plant;

 -- Результат выполнения совпадает с эталоном

/*
Код завода|Завод      |Оборот |Скидки по купонам|Оборот с учетом скидки|Кол-во проданных товаров|Количество чеков|Трафик|Кол-во товаров по акции|Доля товаров со скидкой|Среднее количество товаров в чеке|Коэффициент конверсии магазина, %|Средний чек, руб.|Средняя выручка на посетителя, руб|
----------+-----------+-------+-----------------+----------------------+------------------------+----------------+------+-----------------------+-----------------------+---------------------------------+---------------------------------+-----------------+----------------------------------+
M003      |Магазин №3 |1002880|         21213.14|             981666.86|                    3071|             651|  1058|                    112|3.6%                   |                             4.72|61.53%                           |           1540.5|                             947.9|
M004      |Магазин №4 | 274708|          5995.12|             268712.88|                     755|             168|   671|                     35|4.6%                   |                             4.49|25.04%                           |           1635.2|                             409.4|
M005      |Магазин №5 | 465034|          9106.20|             455927.80|                    1511|             331|   501|                     56|3.7%                   |                             4.56|66.07%                           |           1404.9|                             928.2|
M006      |Магазин №6 | 261909|          5349.58|             256559.42|                     767|             164|  1025|                     35|4.6%                   |                             4.68|16.00%                           |           1597.0|                             255.5|
M007      |Магазин №7 | 222390|          3772.78|             218617.22|                     802|             155|   618|                     30|3.7%                   |                             5.17|25.08%                           |           1434.8|                             359.9|
M009      |Магазин №9 | 206682|          2760.80|             203921.20|                     705|             137|   740|                     18|2.6%                   |                             5.15|18.51%                           |           1508.6|                             279.3|
M010      |Магазин №10| 201874|          5638.64|             196235.36|                     751|             166|  2075|                     29|3.9%                   |                             4.52|8.00%                            |           1216.1|                              97.3|
M012      |Магазин №12| 283460|          5248.24|             278211.76|                     890|             182|  1051|                     40|4.5%                   |                             4.89|17.32%                           |           1557.5|                             269.7|
M014      |Магазин №14| 220784|          3856.12|             216927.88|                     695|             147|   472|                     28|4.0%                   |                             4.73|31.14%                           |           1501.9|                             467.8|
M011      |Магазин №11| 199889|          8036.48|             191852.52|                     731|             157|  1121|                     38|5.2%                   |                             4.66|14.01%                           |           1273.2|                             178.3|
M013      |Магазин №13| 278789|          4511.66|             274277.34|                     918|             167|   614|                     31|3.4%                   |                             5.50|27.20%                           |           1669.4|                             454.1|
M015      |Магазин №15|  58302|          1440.00|              56862.00|                     138|              37|  1085|                      6|4.3%                   |                             3.73|3.41%                            |           1575.7|                              53.7|
M008      |Магазин №8 | 395829|          8944.98|             386884.02|                    1220|             300|   479|                     57|4.7%                   |                             4.07|62.63%                           |           1319.4|                             826.4|
M001      |Магазин №1 | 470294|          9859.26|             460434.74|                    1344|             290|  1018|                     56|4.2%                   |                             4.63|28.49%                           |           1621.7|                             462.0|
M002      |Магазин №2 | 212412|          5169.68|             207242.32|                     637|             161|  1017|                     26|4.1%                   |                             3.96|15.83%                           |           1319.3|                             208.9|
*/
 
-- Создадим таблицу для логирования процесса загрузки и формирования витрины данных
 
create table dcs.logs(
	log_id int NOT NULL,
	log_timestamp timestamp NOT NULL DEFAULT NOW(),
	log_type text NOT NULL,
	log_msg text NOT NULL,
	log_location text NOT NULL,
	is_error bool NULL,
	log_user text NULL DEFAULT "current_user"(),
	constraint pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

-- Создадим последовательность для заполнения таблицы с логамии

CREATE SEQUENCE dcs.log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 999999999999999999
	START 1;

-- Создадим функцию для логирования

CREATE OR REPLACE FUNCTION dcs.f_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
	 
DECLARE
v_log_type text;
v_log_message text;
v_location text;
v_sql text;
v_res text;
 
 
BEGIN

v_log_type = upper(p_log_type);
v_location = lower(p_location);


IF v_log_type NOT IN ('ERROR','INFO') THEN
	RAISE EXCEPTION 'Illegal log type! USE one of: ERROR,INFO';
END IF;


RAISE NOTICE '%: %: <>% Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

v_log_message := replace(p_log_message, '''', '''''');

v_sql := 'INSERT INTO dcs.logs(log_id,log_type,log_msg, log_location, is_error, log_timestamp, log_user)
			VALUES (' ||nextval('dcs.log_id_seq')|| ',
				  ''' || v_log_type || ''',
					' || coalesce('''' || v_log_message || '''', 'empty')|| ',
					' || coalesce('''' || v_location || '''', 'null')|| ',
					' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
					current_timestamp,
					current_user);';

EXECUTE v_sql;

RAISE NOTICE 'INSERT SQL IS: %', v_sql; 

v_res := dblink('adb_server',v_sql);
END;



$$
EXECUTE ON ANY;


/*
Создадим функцию, собирающую витрину данных за день запуска
Это позволит формировать регулярную  отчетность.
Данные также будут копироваться в витрину - внешнюю таблицу Clickhouse
*/

CREATE OR REPLACE FUNCTION dcs.f_sales_traffic_mart(p_start_date timestamp)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$

	
	
 
	 
DECLARE
v_start_date timestamp;
v_gp_table_name varchar;
v_ch_table_name varchar;
v_sql varchar;
v_return int;
v_field text;

BEGIN
v_start_date = p_start_date;
v_gp_table_name := 'dcs.gp_sales_traffic_mart_daily';
v_ch_table_name := 'dcs.ch_sales_traffic_mart';
v_field = '''"Дата"'''; 

  
 v_sql = 'CREATE TABLE IF NOT EXISTS '||v_gp_table_name||' (
	"Дата" date,
	"Код завода" text,
	"Завод" text,
	"Оборот" decimal(17,6),
	"Скидки по купонам" decimal(17,6),
	"Оборот с учетом скидки" decimal(17,6),
	"Кол-во проданных товаров" int,
	"Количество чеков" int,
	"Трафик" decimal(17,6),
	"Кол-во товаров по акции" int,
	"Доля товаров со скидкой" text,
	"Среднее количество товаров в чеке" decimal(17,6),
	"Коэффициент конверсии магазина, %" text,
	"Средний чек, руб." decimal(17,6),
	"Средняя выручка на одного посетителя" decimal(17,6))



	WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)

	DISTRIBUTED RANDOMLY;';
	 	
RAISE NOTICE 'CREATING MART TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING MART TABLE: %', 'DONE';

           
v_sql =	'TRUNCATE '||v_gp_table_name||';';
			
EXECUTE v_sql;
	
RAISE NOTICE 'TRUNCATE MART TABLE: %', 'DONE';




v_sql =  'INSERT INTO '||v_gp_table_name||
	' (WITH bills_cte AS(
				
		SELECT bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
		FROM dcs.bills_item bi 
			JOIN dcs.bills_head bh ON bh.billnum = bi.billnum
			LEFT JOIN dcs.stores s ON bh.plant = s.plant	 
		WHERE bh.calday = '''||v_start_date||'''
		), 
				
		traffic_agg AS (
				
			SELECT t.plant, SUM(t.quantity)::decimal AS traffic
			FROM dcs.traffic t 
			WHERE t."date" = '''||v_start_date||'''
			GROUP BY t.plant 
		),  
				 
		coupon_cte AS(
				
			SELECT  c.coupon_num, bc.plant,
			CASE WHEN p.promo_type = 1 THEN p.discount 
				 WHEN p.promo_type = 2 THEN (bc.rpa_sat/bc.qty * ( p.discount)/100) 
				 ELSE NULL END AS discount,
			rank() OVER(PARTITION BY c.coupon_num ORDER BY bc.billitem) AS rank
			FROM bills_cte bc 
				LEFT JOIN dcs.coupons c ON c.billnum = bc.billnum 
				LEFT JOIN dcs.promos p ON p.id = c.coupon_promo AND p.material = c.material    
		),
				
		coupons_agg AS (
				
			SELECT cc.plant, COUNT(DISTINCT cc.coupon_num) AS discounted_goods, ROUND(SUM(CASE WHEN RANK = 1 THEN cc.discount ELSE NULL END),2) AS discounts   
			FROM coupon_cte cc  
			GROUP by cc.plant
		),
				
		sales_agg AS (
				
			SELECT bc.plant,bc.txt, ROUND(SUM(bc.rpa_sat),0) AS turnover, SUM(bc.qty) AS total_sales, COUNT(DISTINCT bc.billnum) AS total_bills 
			FROM bills_cte bc
			GROUP BY bc.plant, bc.txt
		) 
				  
	
		SELECT '''||v_start_date||'''::date AS "Дата", ta.plant AS "Код завода", sa.txt AS "Завод", sa.turnover "Оборот", ca.discounts AS "Скидки по купонам", sa.turnover - ca.discounts AS "Оборот с учетом скидки",
		sa.total_sales AS "Кол-во проданных товаров",sa.total_bills AS "Количество чеков", ta.traffic AS "Трафик", ca.discounted_goods AS "Кол-во товаров по акции",  
		CONCAT(ROUND(ca.discounted_goods/sa.total_sales::DECIMAL*100,1)::text,''%'') AS "Доля товаров со скидкой", ROUND(sa.total_sales::DECIMAL/sa.total_bills,2) AS "Среднее количество товаров в чеке",
		CONCAT(ROUND(sa.total_bills/ta.traffic*100,2)::text,''%'') AS "Коэффициент конверсии магазина, %", ROUND(sa.turnover/sa.total_bills,1) AS "Средний чек, руб.", 
		ROUND(sa.turnover/ta.traffic,1) AS "Средняя выручка на одного посетителя, руб" 
				
		FROM coupons_agg ca 
			JOIN sales_agg sa ON ca.plant = sa.plant
			JOIN traffic_agg ta ON ta.plant = sa.plant
		ORDER BY ta.plant);';

RAISE NOTICE 'INSERT DAILY MART DATA: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'INSERT DAILY MART DATA: %', 'DONE';

		
EXECUTE 'SELECT COUNT(1) FROM ' || v_gp_table_name INTO v_return;
		 
v_sql = 'CREATE TABLE IF NOT EXISTS '||v_ch_table_name||' (LIKE '||v_gp_table_name||');';
			
RAISE NOTICE 'CLICKHOUSE TABLE IS CREATED: %',v_sql;
	
EXECUTE v_sql;
	
RAISE NOTICE 'CLICKHOUSE TABLE IS CREATED: %','DONE';
		
		 

v_sql =  'INSERT INTO '||v_ch_table_name||' SELECT * FROM '||v_gp_table_name||';';
 		
EXECUTE v_sql;

v_sql = 'ANALYZE '|| v_ch_table_name ||';';
		
EXECUTE v_sql;

PERFORM dcs.f_write_log(p_log_type := 'INFO',
				p_log_message := v_return || ' rows inserted',
				p_location := 'Mart_Calculation');

		 
 
 RETURN v_return;


END;
 

 



$$
EXECUTE ON ANY;
 

/*
Проблема сборки данной витрины заключается в различии между полями учавствующими в joins
и полями распределения, что приведет к появлению  Redistribute motion.
Исключить появление этого перемещения в нашем случае возможно только путем создания временных таблиц вместо CTE
c полями распределения идентичными полям, используемым в Joins. При этом ожидается падение скорости формирования витрины.
 */

-----  Напишим функцию, создающую витрину данных с помощью временных таблиц вместо CTE


CREATE OR REPLACE FUNCTION dcs.f_sales_traffic_mart(p_start_date timestamp)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$

	
	
 
	 
DECLARE
v_start_date timestamp;
v_gp_mart_name varchar;
v_ch_mart_name varchar;
v_traffic_agg_temp varchar;
v_coupons_temp varchar;
v_coupons_agg_temp varchar;
v_bills_temp varchar;
v_bills_agg_temp varchar;
v_sql varchar;
v_return int;
v_field text;
 

BEGIN
v_start_date = p_start_date;
v_gp_mart_name := 'dcs.gp_sales_traffic_mart_daily';
v_ch_mart_name := 'dcs.ch_sales_traffic_mart_daily';
v_traffic_agg_temp := 'traffic_agg_temp';
v_coupons_temp := 'coupons_temp';
v_coupons_agg_temp := 'coupons_agg_temp';
v_bills_temp := 'bills_temp';
v_bills_agg_temp := 'bills_agg_temp';
v_field = '''"Дата"''';
 
    
	
v_sql = 'CREATE TABLE IF NOT EXISTS '||v_gp_mart_name||' (
	"Дата" date,
	"Код завода" text,
	"Завод" text,
	"Оборот" decimal(17,6),
	"Скидки по купонам" decimal(17,6),
	"Оборот с учетом скидки" decimal(17,6),
	"Кол-во проданных товаров" int,
	"Количество чеков" int,
	"Трафик" decimal(17,6),
	"Кол-во товаров по акции" int,
	"Доля товаров со скидкой" text,
	"Среднее количество товаров в чеке" decimal(17,6),
	"Коэффициент конверсии магазина, %" text,
	"Средний чек, руб." decimal(17,6),
	"Средняя выручка на одного посетителя" decimal(17,6))



	WITH (
	appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd)
	DISTRIBUTED RANDOMLY;';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING MART TABLE: %', 'DONE';

			
v_sql = 'CREATE TEMP TABLE '||v_bills_temp||' 
WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
ON COMMIT DROP 
AS (
	SELECT bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
	FROM dcs.bills_item bi 
		LEFT JOIN dcs.bills_head bh ON bh.billnum = bi.billnum
		LEFT JOIN dcs.stores s ON bh.plant = s.plant	 
	WHERE bh.calday = '''||v_start_date||'''
	) 
	DISTRIBUTED BY(billnum, material);';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING TEMP TABLE bills: %', 'DONE';


v_sql = 'CREATE TEMP TABLE '||v_coupons_temp||' 
	WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
	ON COMMIT DROP 
	AS (
		SELECT  c.coupon_num, bt.plant,
		CASE 
			WHEN p.promo_type = 1 THEN p.discount 
			WHEN p.promo_type = 2 THEN (bt.rpa_sat/bt.qty * ( p.discount)/100) 
			ELSE NULL END AS discount,
		rank() OVER(PARTITION BY c.coupon_num ORDER BY bt.billitem) AS rank
		FROM '||v_bills_temp||' bt 
		LEFT JOIN dcs.coupons c ON c.billnum = bt.billnum 
		LEFT JOIN dcs.promos p ON p.id = c.coupon_promo AND p.material = c.material
	) 
	DISTRIBUTED BY(plant);';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING TEMP TABLE coupons_temp: %', 'DONE';


v_sql = 'CREATE TEMP TABLE '||v_coupons_agg_temp||' 
	WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
	ON COMMIT DROP 
	AS (
		SELECT ct.plant, COUNT(DISTINCT ct.coupon_num) AS discounted_goods, ROUND(SUM(CASE WHEN RANK = 1 THEN ct.discount ELSE NULL END),2) AS discounts   
		FROM '||v_coupons_temp||' ct  
		GROUP by ct.plant
	) 
	DISTRIBUTED BY(plant);';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING TEMP TABLE coupons_agg_temp: %', 'DONE';

v_sql = 'CREATE TEMP TABLE '||v_bills_agg_temp||' 
	WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
	ON COMMIT DROP 
	AS (
		SELECT bt.plant,bt.txt, ROUND(SUM(bt.rpa_sat),0) AS turnover, SUM(bt.qty) AS total_sales, COUNT(DISTINCT bt.billnum) AS total_bills 
		FROM '||v_bills_temp||' bt
		GROUP BY bt.plant, bt.txt
	) 
	DISTRIBUTED BY(plant);';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING TEMP TABLE bills_agg_temp: %', 'DONE';
			
v_sql = 'CREATE TEMP TABLE '||v_traffic_agg_temp||' 
	WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
	ON COMMIT DROP 
	AS (
		SELECT t.plant, SUM(t.quantity)::decimal AS traffic
		FROM dcs.traffic t 
		WHERE t.date = '''||v_start_date||'''
		GROUP BY t.plant 
	) 
	DISTRIBUTED BY(plant);';
	 	
RAISE NOTICE 'CREATING TABLE: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'CREATING TEMP TABLE traffic_agg_temp: %', 'DONE';

v_sql =  'INSERT INTO '||v_gp_mart_name|| '  
	(SELECT now()::date AS "Дата", ba.plant AS "Код завода", ba.txt AS "Завод", ba.turnover "Оборот", ca.discounts AS "Скидки по купонам", ba.turnover - ca.discounts AS "Оборот с учетом скидки",
	ba.total_sales AS "Кол-во проданных товаров",ba.total_bills AS "Количество чеков", ta.traffic AS "Трафик", ca.discounted_goods AS "Кол-во товаров по акции",  
	CONCAT(ROUND(ca.discounted_goods/ba.total_sales::DECIMAL*100,1)::text,''%'') AS "Доля товаров со скидкой", ROUND(ba.total_sales::DECIMAL/ba.total_bills,2) AS "Среднее количество товаров в чеке",
	CONCAT(ROUND(ba.total_bills/ta.traffic*100,2)::text,''%'') AS "Коэффициент конверсии магазина, %", ROUND(ba.turnover/ba.total_bills,1) AS "Средний чек, руб.", 
	ROUND(ba.turnover/ta.traffic,1) AS "Средняя выручка на одного посетителя, руб" 
				
	FROM '||v_bills_agg_temp||' ba 
		JOIN  '||v_coupons_agg_temp||' ca ON ba.plant = ca.plant
		JOIN '||v_traffic_agg_temp||'  ta ON ba.plant = ta.plant
	ORDER BY ba.plant);';

RAISE NOTICE 'INSERT DAILY MART DATA: %',v_sql;

EXECUTE v_sql;

	  
RAISE NOTICE 'INSERT DAILY MART DATA: %', 'DONE';

		
		
EXECUTE 'SELECT COUNT(1) FROM ' || v_gp_mart_name INTO v_return;

v_sql = 'CREATE TABLE IF NOT EXISTS '||v_ch_table_name||' (LIKE '||v_gp_table_name||');';
			
RAISE NOTICE 'CLICKHOUSE TABLE IS CREATED: %',v_sql;
	
EXECUTE v_sql;
	
RAISE NOTICE 'CLICKHOUSE TABLE IS CREATED: %','DONE';
		
v_sql =  'INSERT INTO '||v_ch_table_name||' SELECT * FROM '||v_gp_table_name||';';
 		
EXECUTE v_sql;

v_sql = 'ANALYZE '|| v_ch_table_name ||';';
		
EXECUTE v_sql;	 

 PERFORM dcs.f_write_log(p_log_type := 'INFO',
			p_log_message := v_return || 'rows inserted for ',
			p_location := 'Mart_Calculation');
    	  
 		 
		
		
RETURN v_return;
 
END;
 

 



$$
EXECUTE ON ANY;

 
