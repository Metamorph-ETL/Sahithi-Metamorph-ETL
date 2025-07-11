from airflow.decorators import dag
from datetime import  timedelta
from tasks.adhoc.adhoc_drop_duplicates_legacy import  m_adhoc_load_supplier
import pendulum

IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "airflow",
    "retries": 3,                             
    "retry_delay": timedelta(minutes=5),       
}

@dag(
    dag_id="adhoc_job",
    default_args=default_args,   
    catchup=False,
    tags=["METAMORPH","ADHOC"]
)


def etl_process():
   
     m_adhoc_load_supplier()
    
   

    # Set dependencies inside the DAG function
    # sales_task  >> [supplier_performance_task, product_performance_task] 
  

dag_instance = etl_process()