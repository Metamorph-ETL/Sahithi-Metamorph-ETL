from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, APIClient, load_to_postgres, DuplicateValidator
import logging
from pyspark.sql.functions import col,current_date
from datetime import datetime

log = logging.getLogger(__name__)

@task
def m_ingest_data_into_suppliers():
    try:
        # Start Spark session
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
        suppliers_df = spark.createDataFrame(response[data])        
              
        suppliers_df_tgt = suppliers_df\
               .withColumnRenamed(suppliers_df.columns[0], "SUPPLIER_ID")\
               .withColumnRenamed(suppliers_df.columns[1], "SUPPLIER_NAME")\
               .withColumnRenamed(suppliers_df.columns[2], "CONTACT_DETAILS")\
               .withColumnRenamed(suppliers_df.columns[3], "REGION")    
        
        suppliers_legacy_df = suppliers_df_tgt\
                               .withColumn("DAY_DT", current_date())

        suppliers_legacy_df_tgt = suppliers_legacy_df\
                                    .select(
                                        col("DAY_DT"),
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME"),
                                        col("CONTACT_DETAILS"),
                                        col("REGION")
                                    )

        # Validate no duplicates based on SUPPLIER_ID        
        validator = DuplicateValidator()
        validator.validate_no_duplicates(suppliers_df_tgt, key_columns=["SUPPLIER_ID"])   
              
        # Load the cleaned data to PostgreSQL        
        load_to_postgres(suppliers_df_tgt, "raw.suppliers_pre", "overwrite")

        load_to_postgres(suppliers_legacy_df_tgt, "legacy.suppliers", "append")      
    
        return "Suppliers ETL process completed successfully."

    except Exception as e:
        log.error("Suppliers ETL failed: %s", str(e))
        raise AirflowException(f"Suppliers ETL failed: {str(e)}")

    finally:
            spark.stop()
                 
@task
def m_ingest_data_into_products():
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

        # Create DataFrame and rename columns
        products_df = spark.createDataFrame(response[data])         

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
        load_to_postgres(products_df_tgt, "raw.products_pre", "overwrite")

        load_to_postgres(products_legacy_df_tgt, "legacy.products", "append")

        
        return "Products ETL process completed successfully."

    except Exception as e:
        log.error("Products ETL failed: %s", str(e))
        raise AirflowException(f"Products ETL failed: {str(e)}")

    finally:
        spark.stop()
        


@task
def m_ingest_data_into_customers():
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

        # Create DataFrame and rename columns
        customers_df = spark.createDataFrame(response[data])
        

        customers_df_tgt = customers_df\
                    .withColumnRenamed(customers_df.columns[0], "CUSTOMER_ID")\
                    .withColumnRenamed(customers_df.columns[1], "NAME")\
                    .withColumnRenamed(customers_df.columns[2], "CITY")\
                    .withColumnRenamed(customers_df.columns[3], "EMAIL")\
                    .withColumnRenamed(customers_df.columns[4], "PHONE_NUMBER")
        
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
        load_to_postgres(customers_df_tgt, "raw.customers_pre", "overwrite")

        load_to_postgres(customers_legacy_df_tgt, "legacy.customers", "append")

        
        return "Customers ETL process completed successfully."

    except Exception as e:
        log.error(f"Customers ETL failed: {str(e)}")
        raise AirflowException(f"Customers ETL failed: {str(e)}")

    finally:
        spark.stop()
        




@task
def m_ingest_data_into_sales():
    try:
        # Start Spark session
       
        spark = init_spark()

        # API Client to fetch data
        log.info("Fetching sales data from GCS bucket...")            
        today_date = datetime.today().strftime("%Y%m%d")
        
        # Create Spark DataFrame from API data
        csv_file_path = f"gs://meta-morph-flow/{today_date}/sales_{today_date}.csv" 

        sales_df = spark.read.csv(
            csv_file_path,
            header=True,
            sep=",",
            inferSchema=True
        )
        
         
           # Rename columns by position (sir’s logic)
        sales_df_tgt = sales_df\
                .withColumnRenamed(sales_df.columns[0], "SALE_ID")\
               .withColumnRenamed(sales_df.columns[1], "CUSTOMER_ID")\
               .withColumnRenamed(sales_df.columns[2], "PRODUCT_ID")\
               .withColumnRenamed(sales_df.columns[3], "SALE_DATE")\
               .withColumnRenamed(sales_df.columns[4], "QUANTITY")\
               .withColumnRenamed(sales_df.columns[5], "DISCOUNT")\
               .withColumnRenamed(sales_df.columns[6], "SHIPPING_COST")\
               .withColumnRenamed(sales_df.columns[7], "ORDER_STATUS")\
               .withColumnRenamed(sales_df.columns[8], "PAYMENT_MODE")

        
        sales_legacy_df = sales_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        sales_legacy_df_tgt = sales_legacy_df\
                                    .select(
                                        col("DAY_DT"),
                                        col("SALE_ID"),
                                        col("CUSTOMER_ID"),
                                        col("PRODUCT_ID"),
                                        col("SALE_DATE"),
                                        col("QUANTITY"),
                                        col("DISCOUNT"),
                                        col("SHIPPING_COST"),
                                        col("ORDER_STATUS"),
                                        col("PAYMENT_MODE")
                                    )               
                                    
        


        # Validate no duplicates based on SUPPLIER_ID
        validator = DuplicateValidator()
        validator.validate_no_duplicates(sales_df_tgt, key_columns=["SALE_ID"])

        # Load the cleaned data to PostgreSQL
        load_to_postgres(sales_df_tgt, "raw.sales_pre", "overwrite")

        load_to_postgres(sales_legacy_df_tgt, "legacy.sales", "append")
        
        
        return "Sales ETL process completed successfully."

    except Exception as e:
        log.error("Sales ETL failed: %s", str(e))
        raise AirflowException(f"Sales ETL failed: {str(e)}")

    finally:
            spark.stop()

