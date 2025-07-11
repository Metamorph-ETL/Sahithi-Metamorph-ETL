from airflow.decorators import dag
from datetime import  timedelta
from tasks.adhoc.adhoc_column_mismatch_20250711_file import  m_adhoc_into_customers,m_adhoc_into_products
import pendulum

IST = pendulum.timezone("Asia/Kolkata")



@dag(
    dag_id="adhoc_job",
    catchup=False,
    tags=["METAMORPH","ADHOC"]
)


def etl_process():
   
   m_adhoc_into_customers()
   m_adhoc_into_products()

dag_instance = etl_process()