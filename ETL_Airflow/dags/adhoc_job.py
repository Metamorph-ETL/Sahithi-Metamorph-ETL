from airflow.decorators import dag
from datetime import  timedelta
from tasks.adhoc.adhoc_column_mismatch_product_performance   import  m_load_product_performance
import pendulum

IST = pendulum.timezone("Asia/Kolkata")



@dag(
    dag_id="adhoc_job",
    catchup=False,
    tags=["METAMORPH","ADHOC"]
)


def etl_process():

    m_load_product_performance()
   


dag_instance = etl_process()