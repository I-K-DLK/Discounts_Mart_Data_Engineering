# Mart_Data_Engineering
Проект по разработке и наполнению витрины данных сети магазинов и визуализации продаж и трафика.


Проект состоит из нескольких разделов:

1) Создание необходимых таблиц в БД Greenplum, подключение к внешним источникам (PostgreSQL, CSV) c помошью pxf/gpfdist
2) Разработка функций для загрузки данных в созданные таблицы (PL/pgSQL)
3) Оркестрация периодичной загрузки данных в таблицы через Apache Airflow
4) Наполнение таблиц данными из внешних источников (PostgreSQL, CSV) c помошью pxf/gpfdist
5) Создание витрины данных по продажам скидкам и трафику сети магазинов
6) Оркестрация периодичной загрузки данных в витрину через Apache Airflow
7) Создание  витрины данных в СУБД Clickhouse 
8) Разработка SQL скрипта для формирования датасета и визуализации в Apache Superset
  
