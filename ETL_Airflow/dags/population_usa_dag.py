from airflow.decorators import dag
from datetime import datetime

from tasks.usa_population_etl import load_population_data

@dag(dag_id ="etl_taskflow",schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False, tags=['etl', 'spark'])
def etl_task():
    load_population_data()

dag_instance = etl_task()
