from airflow.decorators import task
from airflow.exceptions import AirflowException
from tasks.utils import init_spark, APIClient, load_to_postgres, DuplicateValidator, fetch_env_schema
import logging
from pyspark.sql.functions import col, current_date
from datetime import datetime
from pyspark.sql import Row

log = logging.getLogger(__name__)


@task
def m_ingest_data_into_suppliers(env):
    """
    Extracts supplier data from the API, performs transformation and validation,
    and loads it into PostgreSQL tables (raw and legacy).
    
    Args:
        env (str): Environment name ('prod' or 'dev') to select schema.
    """
    try:
        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']

        spark = init_spark()

        log.info("Fetching supplier data from API...")
        client = APIClient()
        response = client.fetch_data("v1/suppliers")

        data = response.get("data")
        if not data:
            raise AirflowException("No supplier data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} supplier records.")

        # Convert list of dicts to list of Row objects
        rows = [Row(**x) for x in data]
        suppliers_df = spark.createDataFrame(rows)

        # Rename columns for target format
        suppliers_df_tgt = suppliers_df \
            .withColumnRenamed(suppliers_df.columns[0], "SUPPLIER_ID") \
            .withColumnRenamed(suppliers_df.columns[1], "SUPPLIER_NAME") \
            .withColumnRenamed(suppliers_df.columns[2], "CONTACT_DETAILS") \
            .withColumnRenamed(suppliers_df.columns[3], "REGION")

        # Add current date column
        suppliers_legacy_df = suppliers_df_tgt.withColumn("DAY_DT", current_date())

        suppliers_legacy_df_tgt = suppliers_legacy_df.select(
            "DAY_DT", "SUPPLIER_ID", "SUPPLIER_NAME", "CONTACT_DETAILS", "REGION"
        )

        # Check for duplicates by SUPPLIER_ID
        DuplicateValidator().validate_no_duplicates(suppliers_df_tgt, key_columns=["SUPPLIER_ID"])

        # Load data into PostgreSQL
        load_to_postgres(suppliers_df_tgt, f"{raw}.suppliers_pre", "overwrite")
        load_to_postgres(suppliers_legacy_df_tgt, f"{legacy}.suppliers", "append")

        return "Suppliers ETL process completed successfully."

    except Exception as e:
        log.error("Suppliers ETL failed: %s", str(e))
        raise AirflowException(f"Suppliers ETL failed: {str(e)}")

    finally:
        spark.stop()


@task
def m_ingest_data_into_products(env):
    """
    Extracts product data from the API, transforms and validates it,
    and loads it into raw and legacy PostgreSQL tables.

    Args:
        env (str): Environment name ('prod' or 'dev') to select schema.
    """
    try:
        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']

        spark = init_spark()

        log.info("Fetching product data from API...")
        client = APIClient()
        response = client.fetch_data("v1/products")

        data = response.get("data")
        if not data:
            raise AirflowException("No product data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} product records.")
        rows = [Row(**x) for x in data]
        products_df = spark.createDataFrame(rows)

        # Rename columns
        products_df_tgt = products_df \
            .withColumnRenamed(products_df.columns[0], "PRODUCT_ID") \
            .withColumnRenamed(products_df.columns[1], "PRODUCT_NAME") \
            .withColumnRenamed(products_df.columns[2], "CATEGORY") \
            .withColumnRenamed(products_df.columns[3], "SELLING_PRICE") \
            .withColumnRenamed(products_df.columns[4], "COST_PRICE") \
            .withColumnRenamed(products_df.columns[5], "STOCK_QUANTITY") \
            .withColumnRenamed(products_df.columns[6], "REORDER_LEVEL") \
            .withColumnRenamed(products_df.columns[7], "SUPPLIER_ID")

        products_legacy_df = products_df_tgt.withColumn("DAY_DT", current_date())

        products_legacy_df_tgt = products_legacy_df.select(
            "DAY_DT", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY",
            "SELLING_PRICE", "COST_PRICE", "STOCK_QUANTITY", "REORDER_LEVEL", "SUPPLIER_ID"
        )

        DuplicateValidator().validate_no_duplicates(products_df_tgt, key_columns=["PRODUCT_ID"])

        load_to_postgres(products_df_tgt, f"{raw}.products_pre", "overwrite")
        load_to_postgres(products_legacy_df_tgt, f"{legacy}.products", "append")

        return "Products ETL process completed successfully."

    except Exception as e:
        log.error("Products ETL failed: %s", str(e))
        raise AirflowException(f"Products ETL failed: {str(e)}")

    finally:
        spark.stop()


@task
def m_ingest_data_into_customers(env):
    """
    Extracts authenticated customer data from the API, processes it,
    validates for duplicates, and loads into PostgreSQL.

    Args:
        env (str): Environment name ('prod' or 'dev') to select schema.
    """
    try:
        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']

        spark = init_spark()

        log.info("Fetching customer data from API...")
        client = APIClient()
        response = client.fetch_data("v1/customers", auth=True)

        data = response.get("data")
        if not data:
            raise AirflowException("No customer data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} customer records.")
        rows = [Row(**x) for x in data]
        customers_df = spark.createDataFrame(rows)

        customers_df_tgt = customers_df \
            .withColumnRenamed(customers_df.columns[0], "CUSTOMER_ID") \
            .withColumnRenamed(customers_df.columns[1], "NAME") \
            .withColumnRenamed(customers_df.columns[2], "CITY") \
            .withColumnRenamed(customers_df.columns[3], "EMAIL") \
            .withColumnRenamed(customers_df.columns[4], "PHONE_NUMBER")

        customers_legacy_df = customers_df_tgt.withColumn("DAY_DT", current_date())

        customers_legacy_df_tgt = customers_legacy_df.select(
            "DAY_DT", "CUSTOMER_ID", "NAME", "CITY", "EMAIL", "PHONE_NUMBER"
        )

        DuplicateValidator().validate_no_duplicates(customers_df_tgt, key_columns=["CUSTOMER_ID"])

        load_to_postgres(customers_df_tgt, f"{raw}.customers_pre", "overwrite")
        load_to_postgres(customers_legacy_df_tgt, f"{legacy}.customers", "append")

        return "Customers ETL process completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}")
        raise AirflowException(f"Customers ETL failed: {str(e)}")

    finally:
        spark.stop()


@task
def m_ingest_data_into_sales(env):
    """
    Reads daily sales CSV file from GCS, transforms it,
    validates for duplicates, and loads into PostgreSQL tables.

    Args:
        env (str): Environment name ('prod' or 'dev') to select schema.
    """
    try:
        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']

        spark = init_spark()

        log.info("Fetching sales data from GCS bucket...")

        today_date = datetime.today().strftime("%Y%m%d")
        csv_file_path = f"gs://meta-morph-flow/{today_date}/sales_{today_date}.csv"

        sales_df = spark.read.csv(
            csv_file_path,
            header=True,
            sep=",",
            inferSchema=True
        )

        # Rename by column positions
        sales_df_tgt = sales_df \
            .withColumnRenamed(sales_df.columns[0], "SALE_ID") \
            .withColumnRenamed(sales_df.columns[1], "CUSTOMER_ID") \
            .withColumnRenamed(sales_df.columns[2], "PRODUCT_ID") \
            .withColumnRenamed(sales_df.columns[3], "SALE_DATE") \
            .withColumnRenamed(sales_df.columns[4], "QUANTITY") \
            .withColumnRenamed(sales_df.columns[5], "DISCOUNT") \
            .withColumnRenamed(sales_df.columns[6], "SHIPPING_COST") \
            .withColumnRenamed(sales_df.columns[7], "ORDER_STATUS") \
            .withColumnRenamed(sales_df.columns[8], "PAYMENT_MODE")

        sales_legacy_df = sales_df_tgt.withColumn("DAY_DT", current_date())

        sales_legacy_df_tgt = sales_legacy_df.select(
            "DAY_DT", "SALE_ID", "CUSTOMER_ID", "PRODUCT_ID",
            "SALE_DATE", "QUANTITY", "DISCOUNT", "SHIPPING_COST",
            "ORDER_STATUS", "PAYMENT_MODE"
        )

        DuplicateValidator().validate_no_duplicates(sales_df_tgt, key_columns=["SALE_ID"])

        load_to_postgres(sales_df_tgt, f"{raw}.sales_pre", "overwrite")
        load_to_postgres(sales_legacy_df_tgt, f"{legacy}.sales", "append")

        return "Sales ETL process completed successfully."

    except Exception as e:
        log.error("Sales ETL failed: %s", str(e))
        raise AirflowException(f"Sales ETL failed: {str(e)}")

    finally:
        spark.stop()
