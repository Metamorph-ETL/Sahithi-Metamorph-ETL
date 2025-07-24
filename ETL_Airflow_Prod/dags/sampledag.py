from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Python function
def print_hello():
    return "Hello, Airflow!"

# Define the DAG
with DAG(
    'example_dag',
    start_date=datetime(2024, 3, 31),  
    schedule_interval='@daily',  
    catchup=False  
) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )
