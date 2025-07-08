from airflow.decorators import dag
from datetime import  timedelta
from tasks.adhoc.adhoc_load_sales_20250707_file import  m_ingest_data_into_sales , m_load_product_performance , m_load_supplier_performance
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
    sales_task = m_ingest_data_into_sales()
    supplier_performance_task = m_load_supplier_performance()
    product_performance_task = m_load_product_performance()
    
   

    # Set dependencies inside the DAG function
    sales_task  >> [supplier_performance_task, product_performance_task] 
  

dag_instance = etl_process()