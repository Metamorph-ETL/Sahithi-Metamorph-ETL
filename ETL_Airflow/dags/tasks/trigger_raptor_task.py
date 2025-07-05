from Raptor.Raptor import Raptor 
from airflow.decorators import task
import logging 
from utils import init_spark
from dags.secret_key import POSTGRES_PASSWORD

@task
def trigger_raptor():

    spark = init_spark()
    
    raptor_obj = Raptor(spark,username= "postgress",password = POSTGRES_PASSWORD)

    name = raptor_obj.wish("sahithi")

    return name

trigger_raptor()
