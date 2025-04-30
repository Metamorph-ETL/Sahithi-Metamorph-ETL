from airflow.decorators import task
from airflow.exceptions import AirflowException
import requests
from pyspark.sql import SparkSession
import logging
from dags.secret_key import POSTGRES_PASSWORD

@task
def load_population_data():
    log = logging.getLogger(__name__)
    try:
        # Initialize Spark session
        log.info("Initializing Spark session")
        spark = SparkSession.builder \
            .appName("USA_Population_ETL") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .config("spark.master", "local[4]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")
        
        # Extraction: Fetch data from API
        url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        json_data = response.json().get("data", [])
        log.info(f"Extracted {len(json_data)} records")
        
        # Transformation: Convert JSON data to Spark DataFrame and rename columns
        df = spark.createDataFrame(json_data)
        renamed_df = df.selectExpr(
            "ID Nation as id_nation",
            "Nation as nation",
            "ID Year as id_year",
            "Year as year",
            "Population as population",
            "Slug Nation as slug_nation"
        )
        log.info("Data transformed successfully")
        log.info(f"Transformed DataFrame has {renamed_df.count()} rows")
        
        
        

        
          # Step 4: Load to PostgreSQL
        jdbc_url = "jdbc:postgresql://host.docker.internal:5432/data_usa"
        properties = {
            "user": "postgres",
            "password":POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        try:
            renamed_df.write.jdbc(
                url=jdbc_url,
                table="population_data",
                mode="overwrite",
                properties=properties
            )
            log.info("Data written to PostgreSQL successfully.")
        except Exception as e:
            log.error("Failed to write to PostgreSQL", exc_info=True)

        
    except Exception as e:
        raise AirflowException(f"ETL failed: {str(e)}")
    finally:
        if spark:
            spark.stop()
            log.info("Spark session stopped")