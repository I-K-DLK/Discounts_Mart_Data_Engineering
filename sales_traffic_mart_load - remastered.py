import calendar  
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date


# Данные для подключения

DB_CONN = "gp_conn_std9_37"
 
DB_SCHEMA = 'std937'

# В рамках работы на тестовых данных получим список дат за которые 
# будут строиться витрины данных и грузиться в обьединенную таблицу
 
START_DATE = date(2021,1,1)

END_DATE = date(2021,2,28)
 
DATES = [START_DATE + timedelta(days=x) for x in range((END_DATE-START_DATE).days + 1)] 

# Запрос загрузки витрины данных

DB_PROC_MART_LOAD =  "f_sales_traffic_mart"
 
DAILY_MART_LOAD = f"select {DB_SCHEMA}.{DB_PROC_MART_LOAD}(%(current_day)s);"

# Default args  

default_args = {
    'depends_on_past': False,
    'owner': 'std_937',
    'start_date': datetime(2025,1,1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "std937_sales_traffic_mart_load_dag",
    max_active_runs=3,
    schedule_interval='10 19 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    with TaskGroup("mart_load") as task_daily_mart_load:
        for date in DATES: 
            task = PostgresOperator(task_id=f"data_load_{date}",
                                postgres_conn_id=DB_CONN,
                                sql=DAILY_MART_LOAD,
                                parameters={'current_day':f'{date}'}
                                )
    
    task_load_end = DummyOperator(task_id="daily_mart_loaded")
    
    task_end = DummyOperator(task_id="end")
    
    task_start>>task_daily_mart_load>>task_load_end>>task_end