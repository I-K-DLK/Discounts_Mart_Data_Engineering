 
-- Таблицы фактов будем заполнять ежемесячно с помощью метода Замены партиции (Delta Partition)

-- Создадим функцию DELTA PARTITION

-- DROP FUNCTION discounts.f_load_delta_partition(text, text, text, timestamp) 

CREATE OR REPLACE FUNCTION discounts.f_load_delta_partition(p_table text, p_ext_table text, p_partition_key text, p_date timestamp)
														  
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	 


DECLARE


v_temp_table text;
v_table text;
v_ext_table text;
v_params text;
v_dist_key text;
v_where text;
v_where_traffic text;
v_start_date date;
v_end_date date;
v_cnt int8;
v_load_interval interval;
v_sql text;
v_result int;
v_table_oid int;


BEGIN


v_ext_table = p_ext_table;
v_temp_table = p_table||'_temp';
v_load_interval = '1 month' ::interval;
v_start_date := date_trunc('month',p_date);
v_end_date := date_trunc('month',p_date) + v_load_interval;
v_where = p_partition_key ||' >= ''' ||v_start_date|| '''::date AND '||p_partition_key||' < ''' ||v_end_date|| '''::date;';
v_where_traffic = ' to_date('||p_partition_key||',''dd.mm.yyyy'') >= ''' ||v_start_date|| '''::date AND '||' to_date('||p_partition_key||',''dd.mm.yyyy'') < ''' ||v_end_date|| '''::date;';


SELECT c.oid
INTO v_table_oid
FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
WHERE n.nspname || '.' ||c.relname = p_table
LIMIT 1;


IF v_table_oid = 0 OR v_table_oid IS NULL THEN
	v_dist_key = 'DISTRIBUTED RANDOMLY';
ELSE 
	v_dist_key = pg_get_table_distributedby(v_table_oid);
END IF;

SELECT COALESCE('with (' || ARRAY_TO_STRING(reloptions, ', ') || ')','')
FROM pg_class
INTO v_params
WHERE oid = p_table::REGCLASS;

 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
		CREATE TABLE '|| v_temp_table ||' (LIKE '|| p_table ||') ' ||v_params||' '||v_dist_key||';';

EXECUTE v_sql;

RAISE NOTICE 'INSERTED ROWS: %', v_where; 

IF p_table = 'discounts.traffic' THEN
	
	v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT plant, to_date("date",''dd.mm.yyyy'') as date,  time, frame_id, quantity  FROM '|| v_ext_table ||' WHERE '||v_where_traffic||';';
	RAISE NOTICE 'INSERTED ROWS: %', v_sql; 
ELSE  

	v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '|| v_ext_table ||' WHERE '||v_where||';';
	RAISE NOTICE 'INSERTED ROWS: %', v_sql; 
END IF;

EXECUTE v_sql;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

 v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION';
 
EXECUTE v_sql;

RAISE NOTICE 'EXCHANGE PARTITION: %', v_sql;

EXECUTE 'SELECT COUNT(1) FROM ' || p_table ||' WHERE '|| v_where INTO v_result;

RAISE NOTICE 'EXCHANGE PARTITION: %', v_result; 
  
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';';

EXECUTE v_sql;

RAISE NOTICE 'TEMPORARY TABLE IS DELETED'; 

v_sql = 'ANALYZE '|| p_table ||';';

EXECUTE v_sql;

PERFORM discounts.f_write_log(p_log_type := 'INFO',
						   p_log_message := 'end_of_partition_exchange',
						   p_location := 'load_delta_partition');


RETURN v_result;


END;


$$

EXECUTE ON ANY;

 
-- Провeрка распределения
 

SELECT gp_segment_id, count(1)
FROM discounts.bills_head group by gp_segment_id; 
/*
gp_segment_id|count|
-------------+-----+
            4|  556|
            5|  576|
            1|  528|
            0|  519|
            3|  531|
            2|  503|
*/

SELECT gp_segment_id, count(1)
FROM discounts.bills_item group by gp_segment_id; 


/*
gp_segment_id|count|
-------------+-----+
            1| 1737|
            3| 1798|
            4| 1746|
            0| 1713|
            5| 1772|
            2| 1800||
*/


SELECT gp_segment_id, count(1)
FROM discounts.coupons group by gp_segment_id; 


/*
gp_segment_id|count|
-------------+-----+
            5|   98|
            2|  108|
            1|   97|
            3|  103|
            4|   97|
            0|   94|
*/

 
SELECT gp_segment_id, count(1)
FROM discounts.traffic group by gp_segment_id; 


/*
gp_segment_id|count|
-------------+-----+
            1| 2331|
            3| 2328|
            4| 2427|
            0| 2343|
            5| 2321|
            2| 2410|
*/




--Справочники будем заполнять с помощью полной загрузки по методу Full Load (ежемесячно)

-- Функция для загрузки справочников  


CREATE OR REPLACE FUNCTION discounts.f_full_load(p_table text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

 
DECLARE
v_temp_table text;
v_table text;
v_ext_table text;
v_sql text;
v_cnt int8;
v_result int;

BEGIN
 
v_ext_table = p_table||'_ext';
v_temp_table = p_table||'_temp'; 
v_sql = 'TRUNCATE TABLE '|| p_table;

EXECUTE v_sql;
 
v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
		CREATE TABLE '|| v_temp_table ||' (LIKE '|| p_table ||');';
RAISE NOTICE ' TABLE IS: %', v_sql;

EXECUTE v_sql;

v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT * FROM '|| v_ext_table;
RAISE NOTICE 'TEMP TABLE IS: %', v_sql;
EXECUTE v_sql;

GET DIAGNOSTICS v_cnt = ROW_COUNT;

RAISE NOTICE 'INSERTED ROWS: %', v_cnt;

v_sql = 'INSERT INTO '|| p_table ||' SELECT * FROM '|| v_temp_table;

EXECUTE v_sql;


EXECUTE 'SELECT COUNT(1) FROM ' || p_table INTO v_result;
 
RAISE NOTICE 'INSERTED ROWS: %', v_result;

v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';';

EXECUTE v_sql;

RAISE NOTICE 'TEMPORARY TABLE IS DELETED'; 

v_sql = 'ANALYZE '|| p_table ||';';

EXECUTE v_sql;

PERFORM discounts.f_write_log(p_log_type := 'INFO',
						   p_log_message := 'end_of_full load',
						   p_location := 'full_load');
						  
RETURN v_result;


END;
  


$$
EXECUTE ON ANY;


SELECT discounts.f_full_load('discounts.coupons'); -- 597 rows

SELECT discounts.f_full_load('discounts.promos'); -- 8 rows

SELECT discounts.f_full_load('discounts.promo_types'); -- 2 rows

SELECT discounts.f_full_load('discounts.stores'); -- 15 rows

-- Перекос данных при изначальных полях дистрибуции

SELECT (gp_toolkit.gp_skew_coefficient('discounts.bills_head'::regclass)).skccoeff
/*
skccoeff               |
-----------------------+
4.916301108609418898000|
*/
SELECT (gp_toolkit.gp_skew_coefficient('discounts.bills_item'::regclass)).skccoeff
/*
skccoeff               |
-----------------------+
1.658693212216434083000|
*/

SELECT (gp_toolkit.gp_skew_coefficient('discounts.traffic'::regclass)).skccoeff
/*
skccoeff               |
-----------------------+
1.956874901338522631000|
*/
SELECT (gp_toolkit.gp_skew_coefficient('discounts.coupons'::regclass)).skccoeff

/*
skccoeff                |
------------------------+
13.130875385390606231000|
*/

/* У таблицы купонов перекос слишком высок, в идеале нужно попробовать распределение по другому полю или паре полей
Нужно будет также учесть поля джойнов при построении витрины данных для возможного исключения redistribute motion
*/
