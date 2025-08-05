import requests
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import count
from dags.secret_key import POSTGRES_PASSWORD, USERNAME, PASSWORD

log = logging.getLogger(__name__)


def init_spark():
    """
    Initializes and configures a Spark session for use with Google Cloud Storage and PostgreSQL.

    Returns:
        SparkSession: A configured Spark session.
    """
    log.info("Initializing Spark session...")
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    
    # Set service account key for GCS access
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile", 
        "/usr/local/airflow/jars/meta-morph-d-eng-pro-view-key.json"
    )

    log.info("Spark session created successfully.")
    return spark


class APIClient:
    """
    A client to interact with internal API endpoints to fetch data with optional authentication.
    """

    def __init__(self, base_url="http://host.docker.internal:8000"):
        """
        Initializes the APIClient.

        Args:
            base_url (str): The base URL for the API.
        """
        self.base_url = base_url

    def fetch_data(self, api_type: str, auth=False):
        """
        Fetches data from the API endpoint.

        Args:
            api_type (str): The endpoint to fetch data from (e.g., 'v1/data').
            auth (bool): Whether the request requires authentication.

        Returns:
            dict: The JSON response from the API.

        Raises:
            Exception: If request or response fails.
        """
        try:
            headers = {}

            if auth:
                log.info("Generating token to authenticate API request...")

                # Generate token from /token endpoint
                token_response = requests.post(
                    f"{self.base_url}/token",
                    data={'username': USERNAME, 'password': PASSWORD}
                )

                if token_response.status_code != 200:
                    raise Exception(f"Token generation failed: {token_response.text}")

                token = token_response.json().get("access_token")
                if not token:
                    raise Exception("Token not found in response.")
                
                headers["Authorization"] = f"Bearer {token}"
                log.info("Token generated successfully.")

            log.info(f"Fetching data from API endpoint: {self.base_url}/{api_type}")
            response = requests.get(f"{self.base_url}/{api_type}", headers=headers)

            if response.status_code == 404:
                log.error(f"API endpoint {api_type} not found.")
                raise Exception(f"API endpoint {api_type} not found.")

            if response.status_code == 200:
                try:
                    return response.json()
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


class DuplicateException(Exception):
    """
    Custom exception raised when duplicate data is detected.
    """
    def __init__(self, message="Duplicate data detected during validation."):
        super().__init__(message)


class DuplicateValidator:
    """
    Performs validation to ensure data uniqueness based on key columns.
    """

    @classmethod
    def validate_no_duplicates(cls, dataframe, key_columns):
        """
        Validates the DataFrame for duplicate rows based on the specified key columns.

        Args:
            dataframe (DataFrame): The Spark DataFrame to validate.
            key_columns (list): List of column names used for uniqueness check.

        Raises:
            DuplicateException: If duplicate rows are found.
        """
        logging.info("Running duplicate validation on input DataFrame.")

        # Group by key columns and count duplicates
        duplicates = dataframe.groupBy(key_columns).agg(count("*").alias("duplicate_count"))

        # Raise exception if duplicates exist
        if duplicates.filter(duplicates["duplicate_count"] > 1).count() > 0:
            raise DuplicateException
        
        logging.info("No duplicates found. Validation passed.")


def load_to_postgres(df, table_name, mode):
    """
    Loads the Spark DataFrame into a PostgreSQL table.

    Args:
        df (DataFrame): The DataFrame to load.
        table_name (str): The name of the target PostgreSQL table.
        mode (str): Write mode - 'append', 'overwrite', etc.
    """
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/meta_morph"
    properties = {
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    log.info(f"Loading data to PostgreSQL table '{table_name}'...")
    df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)
    log.info("Loaded data into PostgreSQL successfully.")


def read_from_postgres(spark, table_name):
    """
    Reads a table from PostgreSQL into a Spark DataFrame.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): Name of the PostgreSQL table to read.

    Returns:
        DataFrame: Spark DataFrame containing the table data.
    """
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/meta_morph"
    properties = {
        "user": "postgres",
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    log.info(f"Reading table '{table_name}' from PostgreSQL...")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
    log.info(f"Successfully read '{table_name}' from PostgreSQL.")
    return df


def fetch_env_schema(env):
    """
    Returns the schema mapping based on the environment.

    Args:
        env (str): Environment type ('prod' or 'dev').

    Returns:
        dict: Schema mapping dictionary.
    """
    if env == 'prod':
        logging.info("THIS IS PRODUCTION ENVIRONMENT")
        return {
            "raw": "raw",
            "legacy": "legacy"
        }
    else:
        logging.info("THIS IS DEVELOPMENT ENVIRONMENT")
        return {
            "raw": "dev_raw",
            "legacy": "dev_legacy"
        }
