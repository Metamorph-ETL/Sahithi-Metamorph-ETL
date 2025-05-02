from airflow.exceptions import AirflowException
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dags.secret_key import POSTGRES_PASSWORD

# Column mapping: API field -> Database column
column_mapping = {
    "supplier_id": "supplier_id",
    "supplier_name": "supplier_name",
    "contact_details": "contact_details",
    "region": "region"
}

# Initialize Spark session
def init_spark():
    spark = SparkSession.builder \
        .appName("Suppliers_ETL") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
        .config("spark.master", "local[4]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark

# Extract data from Suppliers API
def extract_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("data", [])

# Transform JSON to Spark DataFrame with schema + deduplication
def transform_data(spark, data, api_name="suppliers"):
    if not data:
        raise AirflowException(f"[{api_name}] No data received from API")

    # Convert to DataFrame
    df = spark.createDataFrame(data)

    # Apply column renaming
    exprs = [f"`{old}` as {new}" for old, new in column_mapping.items()]
    renamed_df = df.selectExpr(*exprs)

    # Duplicate check
    id_col = list(column_mapping.values())[0]  # use first mapped column as ID
    count_before = renamed_df.count()
    count_after = renamed_df.dropDuplicates([id_col]).count()

    if count_before > count_after:
        raise AirflowException(f"[{api_name}] Duplicate values found in '{id_col}'")

    # Cast data types (if needed)
    df_clean = renamed_df.select(
        col("supplier_id"),
        col("supplier_name"),
        col("contact_details"),
        col("region")
    )

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

