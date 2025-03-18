 
/*
Superset Query для визуализации отчета в Apache Superset 
Для обеспечения возможности фильтрации lданных по дате будем использовать
Jinja templates 
*/

  
select  "Код завода", "Завод", sum("Оборот") as "Оборот", sum("Скидки по купонам") as "Скидки по купонам", sum("Оборот" - "Скидки по купонам") as "Оборот с учетом скидки",
sum("Кол-во проданных товаров") as "Кол-во проданных товаров", sum("Количество чеков") as "Количество чеков", sum("Трафик") as "Трафик", sum("Кол-во товаров по акции") as "Кол-во товаров по акции",  
round(sum("Кол-во товаров по акции")/sum("Кол-во проданных товаров") *100,1) as "Доля товаров со скидкой,%", round(sum("Кол-во проданных товаров")*1.0/sum("Количество чеков"),2) as "Среднее количество товаров в чеке",
round(( sum("Количество чеков")*1.0/sum("Трафик")*100),2) as "Коэффициент конверсии магазина, %", round(sum("Оборот")/sum("Количество чеков"),1) as "Средний чек, руб.", 
round(sum("Оборот")/sum("Трафик"),1) as "Средняя выручка на одного посетителя, руб" 

from discounts.ch_sales_traffic_mart
where   1 = 1 and (
    {% if from_dttm is not none %}
        "дата" >= date('{{ from_dttm }}') and
    {% endif %}
    {% if to_dttm is not none %}
       "дата" <= date('{{ to_dttm }}') and
    {% endif %}
    true
) -- или "дата" >= '{{ from_dttm }}'  and "дата"  <= '{{ to_dttm }}'
group by "Код завода", "Завод"
order by "Код завода" asc
