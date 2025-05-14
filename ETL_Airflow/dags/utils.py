import requests
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import count
from dags.secret_key import POSTGRES_PASSWORD,USERNAME,PASSWORD 



log = logging.getLogger(__name__)



# Initialize Spark session
def init_spark():
    log.info("Initializing Spark session...")
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-view-key.json")
    logging.info("Spark session Created")
    return spark




class APIClient:
    def __init__(self, base_url="http://host.docker.internal:8000"):
        self.base_url = base_url

    def fetch_data(self, api_type: str, auth=False):
        try:
            headers = {}

            # If authentication is required, generate the token
            if auth:
                log.info("Generating token to authenticate API request...")

                # Generate token by sending a GET request to /token endpoint
                # token_response = requests.get(f"{self.base_url}/token")
                token_response = requests.post(
                    f"{self.base_url}/token",
                    data={'username': USERNAME, 'password': PASSWORD }  # Add the correct data format
                )

                if token_response.status_code != 200:
                    raise Exception(f"Token generation failed: {token_response.text}")

                token_data = token_response.json()
                token = token_data.get("access_token")
                if not token:
                    raise Exception("Token not found in response.")
                
                headers["Authorization"] = f"Bearer {token}"
                log.info("Token generated successfully.")

            log.info(f"Fetching data from API endpoint: {self.base_url}/{api_type}")
            # Make the GET request to the API
            response = requests.get(f"{self.base_url}/{api_type}", headers=headers)
            
            # Check if the API call failed
            if response.status_code == 404:
                log.error(f"API call to {self.base_url}/{api_type} failed. Response: {response.text}")
                raise Exception(f"API endpoint {api_type} not found.")
            
            # Check if the response was successful
            if response.status_code == 200:
                try:
                    log.info(f"Successfully fetched data from {self.base_url}/{api_type}")
                    return response.json()  # Return full JSON response
                except Exception as e:
                    raise Exception(f"Failed to parse JSON: {str(e)}")
            else:
                raise Exception(f"Request failed. Status: {response.status_code}, Response: {response.text}")

        except requests.exceptions.RequestException as e:
            log.error(f"API request failed: {str(e)}")
            raise Exception(f"API request failed: {str(e)}")

        except Exception as e:
            log.error(f"An error occurred: {str(e)}")
            raise Exception(f"An error occurred: {str(e)}")



# Custom exception
class DuplicateException(Exception):
     def __init__(self, message="Duplicate data detected during validation."):
        # Call the base class constructor with the message
        super().__init__(message)

# Duplicate check class
class DuplicateValidator:
    """Performs validation checks to ensure uniqueness in data."""

    @classmethod
    def validate_no_duplicates(cls, dataframe, key_columns):
        logging.info("Running duplicate validation on input DataFrame.")
        duplicates = dataframe.groupBy(key_columns).agg(count("*").alias("duplicate_count"))
        if duplicates.filter(duplicates["duplicate_count"] > 1).count() > 0:
            raise DuplicateException
        logging.info("No duplicates found. Validation passed.")

# Load DataFrame into PostgreSQL
def load_to_postgres(df, table_name):
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/meta_morph"
    properties = {
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite", 
        properties=properties
    )
    log.info("Loaded data into PostgreSQL successfully")
