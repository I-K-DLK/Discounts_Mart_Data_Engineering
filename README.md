# Discounts_Mart_Data_Engineering
Проект по разработке и наполнению витрины данных сети магазинов и визуализации продаж, скидок и трафика.

**Кейс: Сеть магазинов вводит несколько промо-акций со скидочными купонами и руководство требует разработать инструмент мониторинга 
использования скидочных купонов посетителями магазина, а также метрик, связанных  с продажами товаров и посещаемостью магазинов.**


Проект состоит из нескольких разделов:

1) Создание необходимых таблиц в БД Greenplum, подключение к внешним источникам (PostgreSQL, CSV) c помошью pxf/gpfdist
2) Разработка функций для загрузки данных в созданные таблицы (PL/pgSQL)
3) Оркестрация периодичной загрузки данных в таблицы через Apache Airflow
4) Наполнение таблиц данными из внешних источников 
5) Создание витрины данных по продажам скидкам и трафику сети магазинов
6) Оркестрация периодичной загрузки данных в витрину через Apache Airflow
7) Создание  витрины данных в СУБД Clickhouse 
8) Разработка SQL скрипта для формирования датасета и визуализации в Apache Superset
9) Создание Дашборда в Apache Superset
  
