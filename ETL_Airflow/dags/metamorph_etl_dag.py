from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_task import m_ingest_data_into_suppliers, m_ingest_data_into_products, m_ingest_data_into_customers,m_ingest_data_into_Sales

@dag(
    dag_id="ingestion_data_pipeline",
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 2),
    catchup=False,
)
def etl_process():
   m_ingest_data_into_suppliers()
   m_ingest_data_into_products()
   m_ingest_data_into_customers()
   m_ingest_data_into_Sales()
  

dag_instance = etl_process()