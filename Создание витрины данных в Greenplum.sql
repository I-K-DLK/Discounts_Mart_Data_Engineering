/*
Итоговый отчёт представлен в файле во вложении. В примере отчет собран при помощи стандартных формул excel. Исходные данные для формирования отчета содержатся на других листах.

Вам предоставлены данные за 2 месяца, но при проектировании потока вы должны учитывать ситуацию при которой новые данные будут поступать регулярно. Учитывайте данное обстоятельство 

при выборе дистрибуции, партиционирования и типа загрузки.

Период формирования отчета - два месяца, 01.01.2021-28.02.2021. Необходимо, чтобы суммы отчета сходились с версией примера в Excel.

Для итогового проекта требуется:

воспроизвести расчет средствами Greenplum
при сборке витрины пользоваться построением плана запроса и быть готовым объяснить на защите какие операции происходят при сборке витрины.
загрузить витрины данных в Clickhouse 
сформировать отчет в Apache Superset. 
Полностью автоматизировать процесс загрузки данных и сборки витрины с помощью Apache Airflow
Обеспечить возможность формирования отчета помесячно/подневно

*/


--
 
 explain analyze


with bills_cte as (

select bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
from std937.bills_item bi 
	join std937.bills_head bh on bh.billnum = bi.billnum
	left join std937.stores s on bh.plant = s.plant	 
where (bh.calday >= '2021.01.01'  and bh.calday <= '2021.02.28') 
	and (bi.calday >= '2021.01.01'  and bi.calday <= '2021.02.28')),  

traffic_agg as (
 
select t.plant, sum(t.quantity)::decimal as traffic
from std937.traffic t 
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
	left join std937.coupons c on c.billnum = bc.billnum AND c.material = bc.material
	left join std937.promos p on p.id = c.coupon_promo and p.material = c.material
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
 
-------------
-- Оптимизируем запрос

SET optimizer = ON;

explain analyze


with bills_cte as (

select bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
from std937.bills_item bi 
	join std937.bills_head bh on bh.billnum = bi.billnum
	left join std937.stores s on bh.plant = s.plant	 
where (bh.calday >= '2021.01.01'  and bh.calday <= '2021.02.28') 
	and (bi.calday >= '2021.01.01'  and bi.calday <= '2021.02.28')),  

traffic_agg as (
 
select t.plant, sum(t.quantity)::decimal as traffic
from std937.traffic t 
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
	left join std937.coupons c on c.billnum = bc.billnum AND c.material = bc.material
	left join std937.promos p on p.id = c.coupon_promo and p.material = c.material
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


---------------
 analyze std937.bills_head
 analyze std937.bills_item
 analyze std937.coupons
 analyze std937.promo_types
 analyze std937.promos
 analyze std937.stores
 analyze std937.traffic
 
 
-------------------------
-- Создадим таблицу для логирования процесса загрузки и формирования витрины данных
 
create table std937.logs(
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

CREATE SEQUENCE std937.log_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 999999999999999999
	START 1;

-- Создадим функцию для логирования

CREATE OR REPLACE FUNCTION std937.f_write_log(p_log_type text, p_log_message text, p_location text)
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

v_sql := 'INSERT INTO std937.logs(log_id,log_type,log_msg, log_location, is_error, log_timestamp, log_user)
			VALUES (' ||nextval('std937.log_id_seq')|| ',
				  ''' || v_log_type || ''',
					' || coalesce('''' || v_log_message || '''', 'empty')|| ',
					' || coalesce('''' || v_location || '''', 'null')|| ',
					' || CASE WHEN v_log_type = 'ERROR' THEN TRUE ELSE FALSE END || ',
					current_timestamp,
					current_user);';

EXECUTE v_sql;

RAISE NOTICE 'INSERT SQL IS: %', v_sql; 

--v_res := dblink('adb_server',v_sql);
END;



$$
EXECUTE ON ANY;

-- Создадим функцию, собирающую витрину данных за день запуска
-- Это позволит формировать регулярную  отчетность 
 
CREATE OR REPLACE FUNCTION std937.f_sales_traffic_mart(p_start_date timestamp DEFAULT now())
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
v_gp_table_name := 'std937.gp_sales_traffic_mart_daily';
v_ch_table_name := 'std937.ch_sales_traffic_mart';
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



		     WITH (
			appendoptimized=true,
			orientation=column,
			compresslevel=1,
			compresstype=zstd)

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
			FROM std937.bills_item bi 
				LEFT JOIN std937.bills_head bh ON bh.billnum = bi.billnum
				LEFT JOIN std937.stores s ON bh.plant = s.plant	 
			WHERE bh.calday >= '''||v_start_date||'''
				AND bh.calday <= '''||v_start_date||'''
			), 
			
			traffic_agg AS (
			
				SELECT t.plant, SUM(t.quantity)::decimal AS traffic
				FROM std937.traffic t 
				WHERE t."date" >= '''||v_start_date||'''
					AND t."date" <= '''||v_start_date||'''
				GROUP BY t.plant 
			),  
			 
			coupon_cte AS(
			
				SELECT  c.coupon_num, bc.plant,
				CASE WHEN p.promo_type = 1 THEN p.discount 
					 WHEN p.promo_type = 2 THEN (bc.rpa_sat/bc.qty * ( p.discount)/100) 
					 ELSE NULL END AS discount,
				rank() OVER(PARTITION BY c.coupon_num ORDER BY bc.billitem) AS rank
				FROM bills_cte bc 
					LEFT JOIN std937.coupons c ON c.billnum = bc.billnum 
					LEFT JOIN std937.promos p ON p.id = c.coupon_promo AND p.material = c.material    
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

        PERFORM std937.f_write_log(p_log_type := 'INFO',
							  p_log_message := v_return || ' rows inserted',
						      p_location := 'Sales_Traffic_Mart_Calculation');

		 
 
 		RETURN v_return;


END;
 

 



$$
EXECUTE ON ANY;

--SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ch_sales_traffic_mart_daily') AS existense
 
-- Тест сборки витрины для данных за Январь - Февраль 2021 г.

select std937.f_sales_traffic_mart('2025-03-02');

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


-- Сделаем два вида названий витрины данных и представлений : за текущий месяц и за текущий день

CREATE OR REPLACE FUNCTION std937.f_sales_traffic_mart(p_start_date varchar, p_end_date varchar)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$

	
	
 
	 
DECLARE
v_start_date varchar;
v_end_date varchar;
v_table_name varchar;
v_view_name varchar; 
v_sql varchar;
v_return int;
 

 
BEGIN
v_start_date := p_start_date;
v_end_date := p_end_date;
 

IF EXTRACT(DAY FROM (v_end_date::timestamp - v_start_date::timestamp))::int = 1 THEN 
	
	v_table_name = 'std937.sales_traffic_mart_daily';

	v_view_name = 'std937.sales_traffic_view_daily';
	  
ELSE 
	
	v_table_name = 'std937.sales_traffic_mart_monthly';

	v_view_name = 'std937.sales_traffic_view_monthly';
 
END IF;
   
 	v_sql = 'DROP TABLE IF EXISTS ' ||v_table_name||' CASCADE;';  

EXECUTE v_sql;

	RAISE NOTICE 'DROPPING TABLE: %', 'DONE';
    
	
		 v_sql = 'CREATE TABLE '||v_table_name||
		   ' WITH (
			appendoptimized=true,
			orientation=column,
			compresslevel=1,
			compresstype=zstd)

		  	AS
	 	
			WITH bills_cte as(
			
			SELECT bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
			FROM std937.bills_item bi 
				LEFT JOIN std937.bills_head bh ON bh.billnum = bi.billnum
				LEFT JOIN std937.stores s ON bh.plant = s.plant	 
			WHERE bh.calday >= '''||v_start_date||'''
				AND bh.calday <= '''||v_end_date||'''
			), 
			
			traffic_agg as (
			
				SELECT t.plant, SUM(t.quantity)::DECIMAL AS traffic
				FROM std937.traffic t 
				WHERE t."date" >= '''||v_start_date||'''
					AND t."date" <= '''||v_end_date||'''
				GROUP BY t.plant 
			),  

			coupon_cte AS(
			
				SELECT  c.coupon_num, bc.plant,
				CASE WHEN p.promo_type = 1 THEN p.discount 
					 WHEN p.promo_type = 2 THEN (bc.rpa_sat/bc.qty * ( p.discount)/100) 
					 ELSE NULL END AS discount,
				rank() OVER(PARTITION BY c.coupon_num ORDER BY bc.billitem) AS rank
				FROM bills_cte bc 
					LEFT JOIN std937.coupons c ON c.billnum = bc.billnum 
					LEFT JOIN std937.promos p ON p.id = c.coupon_promo AND p.material = c.material    
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
			  

			SELECT ta.plant AS "Код завода", sa.txt AS "Завод", sa.turnover "Оборот", ca.discounts AS "Скидки по купонам", sa.turnover - ca.discounts AS "Оборот с учетом скидки",
			sa.total_sales AS "Кол-во проданных товаров",sa.total_bills AS "Количество чеков", ta.traffic AS "Трафик", ca.discounted_goods AS "Кол-во товаров по акции",  
			CONCAT(ROUND(ca.discounted_goods/sa.total_sales::DECIMAL*100,1)::text,''%'') AS "Доля товаров со скидкой", ROUND(sa.total_sales::DECIMAL/sa.total_bills,2) AS "Среднее количество товаров в чеке",
			CONCAT(ROUND(sa.total_bills/ta.traffic*100,2)::text,''%'') AS "Коэффициент конверсии магазина, %", ROUND(sa.turnover/sa.total_bills,1) AS "Средний чек, руб.", 
			ROUND(sa.turnover/ta.traffic,1) AS "Средняя выручка на одного посетителя, руб" 
			
			FROM coupons_agg ca 
				JOIN sales_agg sa ON ca.plant = sa.plant
				JOIN traffic_agg ta ON ta.plant = sa.plant
			ORDER BY ta.plant

	  		DISTRIBUTED RANDOMLY;';

		RAISE NOTICE 'CREATING TABLE: %',v_sql;

		EXECUTE v_sql;

	  
		RAISE NOTICE 'CREATING TABLE: %', 'DONE';
		
		EXECUTE 'SELECT COUNT(1) FROM ' || v_table_name INTO v_return;
 
        PERFORM std937.f_write_log(p_log_type := 'INFO',
							  p_log_message := v_return || 'rows inserted',
						      p_location := 'sales_traffic_mart_calculation');

    	 
	    v_sql =
		'CREATE OR REPLACE VIEW '||v_view_name||   
		 ' AS SELECT * FROM '||v_table_name||';';		
		
		EXECUTE v_sql;
  		
		RAISE NOTICE 'CREATING VIEW: %', 'DONE';

		PERFORM std937.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'end_of_view_creation',
						      p_location := 'sales_traffic_view_creation');
		
		PERFORM std937.f_write_log(p_log_type := 'INFO',
							  p_log_message := 'end_of_f_sales_traffic_mart',
						      p_location := 'sales_traffic_mart_calculation');
	  
 		RETURN v_return;


END;



$$
EXECUTE ON ANY;

select std937.f_sales_traffic_mart('20210101','20210301'); -- витрина за Январь - Февраль 2021 г.
select std937.f_sales_traffic_mart('20240101','20240102'); --пример ежедневной витрины
select std937.f_sales_traffic_mart('20240101','20240201'); --пример ежемесячной витрины
 
 
DROP TABLE std937.sales_traffic_mart_monthly CASCADE

DROP VIEW std937.sales_traffic_view_daily


-- Создадим таблицу для загрузки копий ежедневной витрины данных





-- Создадим внешнюю таблицу в ClickHouse для загрузки данных витрин из Greenplum

DROP EXTERNAL TABLE std937.ch_sales_traffic_mart_daily

CREATE WRITABLE EXTERNAL TABLE std937.ch_sales_traffic_mart_daily (LIKE std937.sales_traffic_mart_daily
)
LOCATION (
    'pxf://std937.ch_sales_traffic_mart_daily_tmp_$?profile=tkh&url=jdbc:clickhouse://192.168.214.206:8123'
)
FORMAT 'TEXT'
ENCODING 'UTF8';




		  	 

SELECT * FROM std937.ch_sales_traffic_mart_daily

INSERT INTO   std937.ch_sales_traffic_mart_daily(
"Дата",
"Код завода",
"Завод",
"Оборот",
"Скидки по купонам",
"Оборот с учетом скидки",
"Кол-во проданных товаров",
"Количество чеков",
"Трафик",
"Кол-во товаров по акции",
"Доля товаров со скидкой",
"Среднее количество товаров в чеке",
"Коэффициент конверсии магазина, %",
"Средний чек, руб.",
"Средняя выручка на одного посетителя"
) values(
'2021-05-15',
'3453',
'ergt',
45,
4,
6,
567,
566,
65,
56,
'14%',
4,
'55%',
11115,
76665)



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
	
	
create table std937.test (
id int,
text1 text,
text2 text
) with (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
distributed by (id);

insert into std937.test select gen, 'Some text #' || gen::text, 'Some another text #' || gen::text from generate_series(1, 1000000) gen;

CREATE WRITABLE EXTERNAL TABLE std937.test_ext (
        LIKE std937.test
 )
LOCATION ('pxf://test_ext?PROFILE=JDBC&JDBC_DRIVER=com.clickhouse.jdbc.ClickHouseDriver&DB_URL=jdbc:clickhouse://192.168.214.206:8123/std937&USER=std9_37&PASS=e9XhF9F0BHAgDO')
ON ALL
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

INSERT INTO std937.test_ext SELECT * FROM std937.test;
INSERT INTO std937.test_ext VALUES(23,'rfge','ethe');

SELECT * FROM std937.test_ext

DROP EXTERNAL TABLE IF EXISTS traffic_ext;



CREATE WRITABLE EXTERNAL TABLE traffic_ext (
	plant text,
	date text,
	time text,
	frame_id text,
	quantity int
	)

LOCATION ('pxf://gp.traffic?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern')
ON ALL 
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


-----  Напишим функцию создающую витрину данных с помощью временных таблиц вместо CTE


CREATE OR REPLACE FUNCTION std937.f_sales_traffic_mart(p_start_date timestamp DEFAULT now(), p_end_date timestamp DEFAULT now())
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$

	
	
 
	 
DECLARE
v_start_date timestamp;
v_end_date timestamp;
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
v_time_cost varchar;
v_time_start timestamp;
v_time_end timestamp;

BEGIN
v_start_date = p_start_date;
v_end_date = p_end_date;
v_gp_mart_name := 'std937.gp_sales_traffic_mart_daily';
v_ch_mart_name := 'std937.ch_sales_traffic_mart_daily';
v_traffic_agg_temp := 'traffic_agg_temp';
v_coupons_temp := 'coupons_temp';
v_coupons_agg_temp := 'coupons_agg_temp';
v_bills_temp := 'bills_temp';
v_bills_agg_temp := 'bills_agg_temp';
v_field = '''"Дата"''';
v_time_start = clock_timestamp();
    
	
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
			appendoptimized=true,
			orientation=column,
			compresslevel=1,
			compresstype=zstd)

		  	DISTRIBUTED RANDOMLY;';
	 	
		RAISE NOTICE 'CREATING TABLE: %',v_sql;

		EXECUTE v_sql;

	  
		RAISE NOTICE 'CREATING MART TABLE: %', 'DONE';

			
		 v_sql = 'CREATE TEMP TABLE '||v_bills_temp||' 
				  WITH (appendoptimized=true, orientation=column, compresslevel=1, compresstype=zstd) 
				  ON COMMIT DROP 
				  AS (
				  SELECT bh.billnum , bi.billitem, bi.material, bi.qty,netval, bi.tax, bi.rpa_sat, bh.calday, s.plant, s.txt 
				  FROM std937.bills_item bi 
					  LEFT JOIN std937.bills_head bh ON bh.billnum = bi.billnum
					  LEFT JOIN std937.stores s ON bh.plant = s.plant	 
				  WHERE bh.calday >= '''||v_start_date||'''
					  AND bh.calday <= '''||v_end_date||'''
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
					LEFT JOIN std937.coupons c ON c.billnum = bt.billnum 
					LEFT JOIN std937.promos p ON p.id = c.coupon_promo AND p.material = c.material
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
				  FROM std937.traffic t 
				  WHERE t.date >= '''||v_start_date||'''
					AND t.date <= '''||v_end_date||'''
				  GROUP BY t.plant 
				  ) 
				  DISTRIBUTED BY(plant);';
	 	
			RAISE NOTICE 'CREATING TABLE: %',v_sql;

			EXECUTE v_sql;

	  
			RAISE NOTICE 'CREATING TEMP TABLE traffic_agg_temp: %', 'DONE';

			v_sql =  'INSERT INTO '||v_gp_mart_name||
			'  
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

		 

        PERFORM std937.f_write_log(p_log_type := 'INFO',
							  p_log_message := v_return || 'rows inserted for ',
						      p_location := 'Sales_Traffic_Daily_Mart_Calculation');

    	 
 		 
		v_time_end = clock_timestamp();
		
		RAISE NOTICE 'TIME START: %', v_time_start;
		RAISE NOTICE 'TIME END: %', v_time_end;
	    EXECUTE 'SELECT EXTRACT(milliseconds FROM ('''||v_time_end||'''::timestamp)) - EXTRACT(milliseconds FROM ('''||v_time_start||'''::timestamp)) as cost ' INTO v_time_cost;
		 
		
		
		RETURN concat(v_return,'  ', v_time_start,' ',v_time_end,'  ',v_time_cost );
 
END;
 

 



$$
EXECUTE ON ANY;

SELECT statement_clock_timestamp(); 
SELECT std937.f_sales_traffic_mart('2021-01-01','2021-02-28');
 
SELECT EXTRACT(milliseconds FROM ('2025-02-23 22:22:13.689 +0300'::timestamp)) - EXTRACT(milliseconds FROM ('2025-02-23 22:22:12.345 +0300'::timestamp)) as cost
 
TRUNCATE std937.gp_sales_traffic_mart_daily;


CREATE TABLE std937.test_view(
id int,
num int
)
DISTRIBUTED BY (id);

INSERT INTO std937.test_view values(1,2)

SELECT * FROM std937.test_view