from airflow.decorators import dag
from datetime import datetime, timedelta
from tasks.ingestion_task import m_ingest_data_into_suppliers, m_ingest_data_into_products, m_ingest_data_into_customers, m_ingest_data_into_sales
from tasks.m_supplier_performance_task import m_load_supplier_performance
from tasks.m_product_performance_task import m_load_product_performance
from tasks.m_customer_sales_report_task import m_load_customer_sales_report
import pendulum

IST = pendulum.timezone("Asia/Kolkata")

# default_args = {
#     "owner": "airflow",
#     "retries": 3,                             
#     "retry_delay": timedelta(minutes=5),       
# }

@dag(
    dag_id="ingestion_data_pipeline", 
    start_date=datetime(2025, 7, 6, tzinfo=IST),
    catchup=False,
    tags=["METAMORPH"]
)


def etl_process():
    supplier_task = m_ingest_data_into_suppliers()
    product_task = m_ingest_data_into_products()
    customer_task = m_ingest_data_into_customers()
    sales_task = m_ingest_data_into_sales()
    supplier_performance_task = m_load_supplier_performance()
    product_performance_task = m_load_product_performance()
    customer_sales_report_task = m_load_customer_sales_report()
   

    # Set dependencies inside the DAG function
    [supplier_task, product_task, customer_task, sales_task]  >> supplier_performance_task >>  product_performance_task >> customer_sales_report_task
  

dag_instance = etl_process()