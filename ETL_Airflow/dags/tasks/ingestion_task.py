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
        
        # Create Spark DataFrame and clean data
        df = spark.createDataFrame(data)
        df_clean = df.select("SUPPLIER_ID", "SUPPLIER_NAME", "CONTACT_DETAILS", "REGION")
        
        
        # Remove duplicates based on SUPPLIER_ID
        df_clean = df_clean.dropDuplicates(["SUPPLIER_ID"])
        DuplicateValidator.validate_no_duplicates(df_clean, key_columns=["SUPPLIER_ID"])

        # Load the cleaned data to PostgreSQL
        log.info("Loading data into PostgreSQL...")
        load_to_postgres(df_clean, "raw.suppliers")
        
        log.info("Suppliers ETL process completed successfully.")
        return "Suppliers ETL process completed successfully."

    except Exception as e:
        log.error("Suppliers ETL failed: %s", str(e))
        raise AirflowException(f"Suppliers ETL failed: {str(e)}")

    finally:
        # Ensure Spark session is stopped if it was started
       
            spark.stop()
            log.info("Spark session stopped.")


@task
def m_ingest_data_into_products():
    try:
        # Start Spark session
        log.info("Initializing Spark session for products...")
        spark = init_spark()

        # API Client to fetch data
        log.info("Fetching product data from API...")
        client = APIClient()
        response = client.fetch_data("v1/products")

        # Validate data presence
        data = response.get("data")
        if not data:
            raise AirflowException("No product data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} product records.")

        # Create Spark DataFrame and clean data
        df = spark.createDataFrame(data)
        
        # Replace these columns with actual column names returned by your API
        for col_name in df.columns:
               df = df.withColumnRenamed(col_name, col_name.upper())

# Now select with uppercase names
        df_clean = df.select(
               "product_id",
               "product_name",
               "category",
               "price",
               "stock_quantity",
               "reorder_level",  
               "supplier_id"
)
        
        # Remove duplicates
        df_clean = df_clean.dropDuplicates(["PRODUCT_ID"])
        DuplicateValidator.validate_no_duplicates(df_clean, key_columns=["PRODUCT_ID"])

        # Load to PostgreSQL
        log.info("Loading products data into PostgreSQL...")
        load_to_postgres(df_clean, "raw.products")

        log.info("Products ETL process completed successfully.")
        return "Products ETL process completed successfully."

    except Exception as e:
        log.error("Products ETL failed: %s", str(e))
        raise AirflowException(f"Products ETL failed: {str(e)}")

    finally:
        spark.stop()
        log.info("Spark session for products stopped.")

@task
def m_ingest_data_into_customers():
    try:
        # Start Spark session
        log.info("Initializing Spark session for customers...")
        spark = init_spark()

        # Instantiate APIClient with the base URL
        client = APIClient()  # Replace with your base URL
        
        # Fetch customer data from API (using authentication)
        log.info("Fetching customer data from API...")
        response = client.fetch_data("v1/customers", auth=True)
        
        # Check if data is received
        data = response.get("data")
        if not data:
            raise AirflowException("No customer data received or missing 'data' key in response.")
        
        log.info(f"Received {len(data)} customer records.")

        # Create Spark DataFrame and clean data
        df = spark.createDataFrame(data)
        
        # Ensure all columns are uppercase
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.upper())
        
        # Select relevant columns
        df_clean = df.select("CUSTOMER_ID", "NAME", "CITY", "EMAIL", "PHONE_NUMBER")
        
        # Remove duplicates based on CUSTOMER_ID
        df_clean = df_clean.dropDuplicates(["CUSTOMER_ID"])
        
        # Validate that there are no duplicates (you can define your own validation rules)
        DuplicateValidator.validate_no_duplicates(df_clean, key_columns=["CUSTOMER_ID"])

        # Load the cleaned data into PostgreSQL
        log.info("Loading customer data into PostgreSQL...")
        load_to_postgres(df_clean, "raw.customers")

        log.info("Customers ETL process completed successfully.")
        return "Customers ETL process completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}")
        raise AirflowException(f"Customers ETL failed: {str(e)}")

    finally:
        # Ensure Spark session is stopped
        spark.stop()
        log.info("Spark session for customers stopped.")