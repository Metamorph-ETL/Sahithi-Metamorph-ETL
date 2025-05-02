from airflow.exceptions import AirflowException
import requests
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import col
from dags.secret_key import POSTGRES_PASSWORD

log = logging.getLogger(__name__)
# Initialize Spark session
def init_spark():
    spark = SparkSession.builder \
        .appName("Suppliers_ETL") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
        .config("spark.master", "local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    log.info("Spark session initialized")
    return spark

class APIClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_data(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json().get("data", [])
            if not data:
                raise AirflowException(f"No data received from API: {url}")
            return data
        except requests.RequestException as e:
            raise AirflowException(f"API request failed: {str(e)}")

# Transform JSON to Spark DataFrame and check for duplicates
def transform_data(spark, data, api_name="suppliers"):
    df = spark.createDataFrame(data)

    # Check for duplicates on 'supplier_id'
    count_before = df.count()
    df_dedup = df.dropDuplicates(["supplier_id"])
    count_after = df_dedup.count()

    if count_before > count_after:
        raise AirflowException(f"[{api_name}] Duplicate supplier_id values found.")

    # Select required columns (if needed, adjust here)
    df_clean = df_dedup.select("supplier_id", "supplier_name", "contact_details", "region")

    return df_clean

# Load DataFrame into PostgreSQL
def load_to_postgres(df):
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
    properties = {
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(
        url=jdbc_url,
        table="raw.supplier",
        mode="overwrite",
        properties=properties
    )
    
    log.info("Loaded data into PostgreSQL successfully")
