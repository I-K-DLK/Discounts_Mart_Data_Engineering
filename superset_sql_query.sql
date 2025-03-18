---------------
--Superset Query 


 with bills_cte as(

select bh.billnum as billnum, bi.billitem as billitem, bi.material as material, bi.qty as qty, netval, bi.tax as tax, bi.rpa_sat as rpa_sat,  bh.calday as calday, s.plant as plant, s.txt as txt 
from discounts.ch_bills_item_ext bi 
	join discounts.ch_bills_head_ext bh on bh.billnum = bi.billnum
	left join discounts.ch_stores_ext s on bh.plant = s.plant	 
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
	 join discounts.ch_coupons_ext  c on c.material = bc.material and c.billnum = bc.billnum and c.calday = bc.calday
	 join discounts.ch_promos_ext  p on p.id = c.coupon_promo and p.material = c.material    
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
from discounts.ch_traffic_ext  t 
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
