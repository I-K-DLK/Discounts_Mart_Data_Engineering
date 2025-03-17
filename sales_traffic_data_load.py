from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
  

# Данные для подключения
  
DB_CONN = "gp_conn_std9_37"

DB_SCHEMA = 'std937'

 
# Загрузка данных через Delta partition 
  
CURRENT_YEAR = datetime.now().year

CURRENT_MONTH = datetime.now().month

FIRST_DAY  = datetime(CURRENT_YEAR,CURRENT_MONTH,1)
 
DB_PROC_DELTA_LOAD = 'f_load_delta_partition'

DB_PROC_FULL_LOAD = 'f_full_load'
 
DELTA_TABLES = ['std937.bills_head', 'std937.bills_item', 'std937.traffic']
 
PARTITION_KEYS = {'std937.bills_head':'calday', 'std937.bills_item':'calday', 'std937.traffic':'date'}

DELTA_PARTITION_QUERY = f"select {DB_SCHEMA}.{DB_PROC_DELTA_LOAD}(%(table)s,%(external_table)s,%(partition_key)s,%(start_date)s);"


# Загрузка данных через Full load 
 

DB_PROC_FULL_LOAD = 'f_full_load'
 
 
FULL_LOAD_TABLES = ['std937.coupons','std937.promo_types','std937.promos','std937.stores']

FULL_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_FULL_LOAD}(%(table_name)s);"
 
 
# Default args

default_args = {
    'depends_on_past': False,
    'owner': 'std_937',
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "std937_f_sales_traffic_data_load_dag",
    max_active_runs=3,
    schedule_interval= '50 23 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    
                                    
    with TaskGroup("delta_load") as delta_load:
        for table in DELTA_TABLES:
            task = PostgresOperator(task_id=f"data_delta_load_{table}",
                                postgres_conn_id = DB_CONN,
                                sql=DELTA_PARTITION_QUERY,
                                parameters={'table':f'{table}','external_table':f'''{table + '_ext'}''',
                                            'partition_key':f'{PARTITION_KEYS[table]}','start_date':f'{FIRST_DAY}'}
                                )
 
    task_delta_report = DummyOperator(task_id="delta_exchanged")
    
    with TaskGroup("full_load") as full_load:
        for table in FULL_LOAD_TABLES: 
            task = PostgresOperator(task_id=f"data_full_load_{table}",
                                postgres_conn_id=DB_CONN,
                                sql=FULL_LOAD_QUERY,
                                parameters={'table_name':f'{table}'}
                                )
    
    task_full_report = DummyOperator(task_id="dictionaries_loaded")
    
    task_end = DummyOperator(task_id="end")
    
    
    task_start>>delta_load>>task_delta_report>>full_load>>task_full_report>>task_end