from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, APIClient, load_to_postgres, DuplicateValidator
import logging

log = logging.getLogger(__name__)

@task
def m_ingest_data_into_suppliers():
    try:
        # Start Spark session
        log.info("Initializing Spark session...")
        spark = init_spark()

        # API Client to fetch data
        log.info("Fetching supplier data from API...")
        client = APIClient()
        response = client.fetch_data("v1/suppliers")
        
        # Check if the 'data' key exists in the response
        data = response.get("data")
        if not data:
            raise AirflowException("No supplier data received or missing 'data' key in response.")
        
        log.info(f"Received {len(data)} supplier records.")
        
        # Create Spark DataFrame from API data
        df = spark.createDataFrame(data)

        # Rename columns to uppercase
        suppliers_df = (
            df.withColumnRenamed("supplier_id", "SUPPLIER_ID")
              .withColumnRenamed("supplier_name", "SUPPLIER_NAME")
              .withColumnRenamed("contact_details", "CONTACT_DETAILS")
              .withColumnRenamed("region", "REGION")
              .select("SUPPLIER_ID", "SUPPLIER_NAME", "CONTACT_DETAILS", "REGION")
        )

        # Validate no duplicates based on SUPPLIER_ID
        DuplicateValidator.validate_no_duplicates(suppliers_df, key_columns=["SUPPLIER_ID"])

        # Load the cleaned data to PostgreSQL
        log.info("Loading data into PostgreSQL...")
        load_to_postgres(suppliers_df, "raw.suppliers")
        
        log.info("Suppliers ETL process completed successfully.")
        return "Suppliers ETL process completed successfully."

    except Exception as e:
        log.error("Suppliers ETL failed: %s", str(e))
        raise AirflowException(f"Suppliers ETL failed: {str(e)}")

    finally:
            spark.stop()
            log.info("Spark session for suppliers completed.")
       
@task
def m_ingest_data_into_products():
    try:
        # Start Spark session
        log.info("Initializing Spark session for products...")
        spark = init_spark()

        # Fetch product data
        log.info("Fetching product data from API...")
        client = APIClient()
        response = client.fetch_data("v1/products")

        # Validate response
        data = response.get("data")
        if not data:
            raise AirflowException("No product data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} product records.")

        # Create DataFrame and rename columns
        df = spark.createDataFrame(data)
        products_df = (
            df.withColumnRenamed("product_id", "PRODUCT_ID")
              .withColumnRenamed("product_name", "PRODUCT_NAME")
              .withColumnRenamed("category", "CATEGORY")
              .withColumnRenamed("price", "PRICE")
              .withColumnRenamed("stock_quantity", "STOCK_QUANTITY")
              .withColumnRenamed("reorder_level", "REORDER_LEVEL")
              .withColumnRenamed("supplier_id", "SUPPLIER_ID")
              .select("PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QUANTITY", "REORDER_LEVEL", "SUPPLIER_ID")
        )

        # Remove and validate duplicates
        products_df = products_df.dropDuplicates(["PRODUCT_ID"])
        DuplicateValidator.validate_no_duplicates(products_df, key_columns=["PRODUCT_ID"])

        # Load data
        log.info("Loading products data into PostgreSQL...")
        load_to_postgres(products_df, "raw.products")

        log.info("Products ETL process completed successfully.")
        return "Products ETL process completed successfully."

    except Exception as e:
        log.error("Products ETL failed: %s", str(e))
        raise AirflowException(f"Products ETL failed: {str(e)}")

    finally:
        spark.stop()
        log.info("Spark session for products completed.")


@task
def m_ingest_data_into_customers():
    try:
        # Start Spark session
        log.info("Initializing Spark session for customers...")
        spark = init_spark()

        # Fetch customer data
        client = APIClient()
        log.info("Fetching customer data from API...")
        response = client.fetch_data("v1/customers", auth=True)

        data = response.get("data")
        if not data:
            raise AirflowException("No customer data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} customer records.")

        # Create DataFrame and rename columns
        df = spark.createDataFrame(data)
        customers_df = (
            df.withColumnRenamed("customer_id", "CUSTOMER_ID")
              .withColumnRenamed("name", "NAME")
              .withColumnRenamed("city", "CITY")
              .withColumnRenamed("email", "EMAIL")
              .withColumnRenamed("phone_number", "PHONE_NUMBER")
              .select("CUSTOMER_ID", "NAME", "CITY", "EMAIL", "PHONE_NUMBER")
        )

        # Remove and validate duplicates
        customers_df = customers_df.dropDuplicates(["CUSTOMER_ID"])
        DuplicateValidator.validate_no_duplicates(customers_df, key_columns=["CUSTOMER_ID"])

        # Load data
        log.info("Loading customer data into PostgreSQL...")
        load_to_postgres(customers_df, "raw.customers")

        log.info("Customers ETL process completed successfully.")
        return "Customers ETL process completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}")
        raise AirflowException(f"Customers ETL failed: {str(e)}")

    finally:
        spark.stop()
        log.info("Spark session for customers completed.")
