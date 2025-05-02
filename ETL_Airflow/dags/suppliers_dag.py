from airflow.decorators import dag
from datetime import datetime
from tasks.etl_suppliers import load_suppliers_data 

@dag(
    dag_id="etl_suppliers_dag",
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1), 
    catchup=False,
    tags=["etl", "suppliers"]
)
def etl_suppliers():
    load_suppliers_data()

dag_instance = etl_suppliers()