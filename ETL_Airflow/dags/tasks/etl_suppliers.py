import os
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark,transform_data, load_to_postgres
from utils import APIClient
from secret_key import SUPPLIERS_API_BASE_URL

# Airflow task for Suppliers ETL
@task
def load_suppliers_data():
    spark = None

    try:
        # Initialize Spark session (logging moved to init_spark function)
        spark = init_spark()

        # Extract data from API (logging moved to extract_data function)
        api_url = SUPPLIERS_API_BASE_URL
        api_client = APIClient(api_url)
        # Extract data using the APIClient instance
        data = api_client.get_data("suppliers")

        # Transform data (logging moved to transform_data function)
        transformed_df = transform_data(spark, data, api_name="suppliers")

        # Load data into PostgreSQL (logging moved to load_to_postgres function)
        load_to_postgres(transformed_df)
        return "Suppliers ETL process completed successfully"

    except Exception as e:
        # Handle error
        raise AirflowException(f"ETL failed: {str(e)}")

    finally:
        if spark:
            # Stop Spark session (logging moved to init_spark function)
            spark.stop()
