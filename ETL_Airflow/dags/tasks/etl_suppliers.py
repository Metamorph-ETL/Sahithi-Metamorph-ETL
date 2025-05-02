import logging
from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark,extract_data,transform_data,load_to_postgres
# Airflow task for Suppliers ETL
@task
def load_suppliers_data():
    log = logging.getLogger(__name__)
    spark = None

    try:
        log.info("Starting ETL for suppliers...")

        spark = init_spark()
        log.info("Spark session initialized")

        # Extract
        url = "http://host.docker.internal:8000/v1/suppliers"
        data = extract_data(url)
        log.info(f"Extracted {len(data)} supplier records")

        # Transform
        transformed_df = transform_data(spark, data, api_name="suppliers")
        log.info("Data transformed successfully")
        log.info(f"Final row count: {transformed_df.count()}")

        # Load
        load_to_postgres(transformed_df)
        log.info("Loaded data into PostgreSQL successfully")

    except Exception as e:
        log.error("Suppliers ETL failed", exc_info=True)
        raise AirflowException(f"ETL failed: {str(e)}")

    finally:
        if spark:
            spark.stop()
            log.info("Spark session stopped")
