from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, APIClient, load_to_postgres, DuplicateValidator
import logging
from pyspark.sql.functions import col, current_date
from pyspark.sql import Row


log = logging.getLogger(__name__)

@task
def m_adhoc_into_customers():
    try:
        # Start Spark session
        spark = init_spark()

        # Fetch customer data
        client = APIClient()
        log.info("Fetching customer data from API...")
        response = client.fetch_data("v1/customers", auth=True)

        data = response.get("data")
        if not data:
            raise AirflowException("No customer data received or missing 'data' key in response.")

        log.info(f"Received {len(data)} customer records.")
        rows = [Row(**x) for x in response["data"]]
        log.info(rows)

        
        # Create DataFrame and rename columns
        customers_df = spark.createDataFrame(rows)
        customers_df.show()

        customers_df_tgt = customers_df\
                .withColumnRenamed(customers_df.columns[0], "CUSTOMER_ID")\
                .withColumnRenamed(customers_df.columns[1], "NAME")\
                .withColumnRenamed(customers_df.columns[2], "CITY")\
                .withColumnRenamed(customers_df.columns[3], "EMAIL")\
                .withColumnRenamed(customers_df.columns[4], "PHONE_NUMBER")
        log.info("Fetching customer data from API...")
    
        
        
        customers_legacy_df = customers_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        
        customers_legacy_df_tgt = customers_legacy_df\
                                    .select(
                                        col("DAY_DT"),
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY"),
                                        col("EMAIL"),
                                        col("PHONE_NUMBER")
                                    )                                 
                           
        
       # Validate no duplicates based on CUSTOMERS_ID
        validator = DuplicateValidator()
        validator.validate_no_duplicates(customers_df_tgt, key_columns=["CUSTOMER_ID"])
        # Load data
        load_to_postgres(customers_df_tgt, "dev_raw.customers_pre", "overwrite")

        load_to_postgres(customers_legacy_df_tgt, "dev_legacy.customers", "append")

        
        return "Customers ETL process completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}")
        raise AirflowException(f"Customers ETL failed: {str(e)}")

    finally:
        spark.stop()
        

                
@task
def m_adhoc_into_products():
    try:
        # Start Spark session
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
        rows = [Row(**x) for x in response["data"]]
        log.info(rows)

        
      


        # Create DataFrame and rename columns
        products_df = spark.createDataFrame(rows)  
        products_df.show()       

        products_df_tgt = products_df\
                    .withColumnRenamed(products_df.columns[0], "PRODUCT_ID")\
                    .withColumnRenamed(products_df.columns[1], "PRODUCT_NAME")\
                    .withColumnRenamed(products_df.columns[2], "CATEGORY")\
                    .withColumnRenamed(products_df.columns[3], "SELLING_PRICE")\
                    .withColumnRenamed(products_df.columns[4], "COST_PRICE")\
                    .withColumnRenamed(products_df.columns[5], "STOCK_QUANTITY")\
                    .withColumnRenamed(products_df.columns[6], "REORDER_LEVEL")\
                    .withColumnRenamed(products_df.columns[7], "SUPPLIER_ID")
        
        products_legacy_df = products_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        products_legacy_df_tgt = products_legacy_df\
                                   .select(
                                       col("DAY_DT"),
                                       col("PRODUCT_ID"), 
                                       col("PRODUCT_NAME"),
                                       col("CATEGORY"),
                                       col("SELLING_PRICE"),
                                       col("COST_PRICE"),
                                       col("STOCK_QUANTITY"),
                                       col("REORDER_LEVEL"),
                                       col("SUPPLIER_ID")
                                    ) 
                                    
                                   
      

        # Validate no duplicates based on PRODUCTS_ID
        validator = DuplicateValidator()
        validator.validate_no_duplicates(products_df_tgt, key_columns=["PRODUCT_ID"])


        # Load data       
        load_to_postgres(products_df_tgt, "dev_raw.products_pre", "overwrite")

        load_to_postgres(products_legacy_df_tgt, "dev_legacy.products", "append")

        
        return "Products ETL process completed successfully."

    except Exception as e:
        log.error("Products ETL failed: %s", str(e))
        raise AirflowException(f"Products ETL failed: {str(e)}")

    finally:
        spark.stop()
        


