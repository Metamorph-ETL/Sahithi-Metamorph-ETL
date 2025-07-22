from airflow.decorators import dag
from datetime import datetime, timedelta
from tasks.ingestion_task import m_ingest_data_into_suppliers, m_ingest_data_into_products, m_ingest_data_into_customers, m_ingest_data_into_sales
from tasks.m_supplier_performance_task import m_load_supplier_performance
from tasks.m_product_performance_task import m_load_product_performance
from tasks.m_customer_sales_report_task import m_load_customer_sales_report
import pendulum
import os 

IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "airflow",
    "retries": 3,                             
    "retry_delay": timedelta(minutes=2),       
}

@dag(
    dag_id="ingestion_data_pipeline",
    default_args= default_args,
    catchup=False,
    tags=["METAMORPH"]
)


def etl_process():
    env  = os.getenv('ENV','dev')
    
    
    supplier_task = m_ingest_data_into_suppliers(env)
    product_task = m_ingest_data_into_products(env)
    customer_task = m_ingest_data_into_customers(env)
    sales_task = m_ingest_data_into_sales(env)
    supplier_performance_task = m_load_supplier_performance(env)
    product_performance_task = m_load_product_performance(env)
    customer_sales_report_task = m_load_customer_sales_report(env)
   

    # Set dependencies inside the DAG function
    [supplier_task, product_task, customer_task] >> sales_task  >> supplier_performance_task >>  product_performance_task >> customer_sales_report_task
  

dag_instance = etl_process()