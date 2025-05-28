from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, APIClient, load_to_postgres, DuplicateValidator
import logging
from pyspark.sql.functions import col,current_date



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
        df = spark.createDataFrame(data)

        # Rename columns to uppercase
        suppliers_df = df\
                        .withColumnRenamed("supplier_id", "SUPPLIER_ID")\
                        .withColumnRenamed("supplier_name", "SUPPLIER_NAME")\
                        .withColumnRenamed("contact_details", "CONTACT_DETAILS")\
                        .withColumnRenamed("region", "REGION")
              
        
        suppliers_df_tgt = suppliers_df\
                            .select(
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )      
        suppliers_df_legacy = suppliers_df\
                               .withColumn("DAY_DT", current_date())

        suppliers_df_tgt_legacy = suppliers_df_legacy\
                                    .select(
                                        col("DAY_DT"),
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME"),
                                        col("CONTACT_DETAILS"),
                                        col("REGION")
                                    )

        # Validate no duplicates based on SUPPLIER_ID        
        validator = DuplicateValidator()
        validator.validate_no_duplicates(df, key_columns=["SUPPLIER_ID"])
       
              


        # Load the cleaned data to PostgreSQL        
        load_to_postgres(suppliers_df_tgt, "raw.suppliers_pre", "overwrite")

        load_to_postgres(suppliers_df_tgt_legacy, "legacy.suppliers", "append")
        
       
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
        df = spark.createDataFrame(data)
        products_df = df\
                        .withColumnRenamed("product_id", "PRODUCT_ID")\
                        .withColumnRenamed("product_name", "PRODUCT_NAME")\
                        .withColumnRenamed("category", "CATEGORY")\
                        .withColumnRenamed("selling_price", "SELLING_PRICE")\
                        .withColumnRenamed("cost_price", "COST_PRICE")\
                        .withColumnRenamed("stock_quantity", "STOCK_QUANTITY")\
                        .withColumnRenamed("reorder_level", "REORDER_LEVEL")\
                        .withColumnRenamed("supplier_id", "SUPPLIER_ID")\
                        
                
        

        products_df_tgt = products_df\
                            .select(                       
                                col("PRODUCT_ID"), 
                                col("PRODUCT_NAME"),
                                col("CATEGORY"),
                                col("SELLING_PRICE"),
                                col("COST_PRICE"),
                                col("STOCK_QUANTITY"),
                                col("REORDER_LEVEL"),
                                col("SUPPLIER_ID"),                               
                            )
        products_df_legacy = products_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        products_df_tgt_legacy = products_df_legacy\
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
        validator.validate_no_duplicates(df, key_columns=["PRODUCT_ID"])


        # Load data       
        load_to_postgres(products_df_tgt, "raw.products_pre", "overwrite")

        load_to_postgres(products_df_tgt_legacy, "legacy.products", "append")

        
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
        df = spark.createDataFrame(data)
        customers_df = df\
                        .withColumnRenamed("customer_id", "CUSTOMER_ID")\
                        .withColumnRenamed("name", "NAME")\
                        .withColumnRenamed("city", "CITY")\
                        .withColumnRenamed("email", "EMAIL")\
                        .withColumnRenamed("phone_number", "PHONE_NUMBER")
                
        

        customers_df_tgt = customers_df\
                            .select(
                                col("CUSTOMER_ID"),
                                col("NAME"),
                                col("CITY"),
                                col("EMAIL"),
                                col("PHONE_NUMBER")
                            )
        customers_df_legacy = customers_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        
        customers_df_tgt_legacy = customers_df_legacy\
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
        validator.validate_no_duplicates(df, key_columns=["CUSTOMER_ID"])
        # Load data
        load_to_postgres(customers_df_tgt, "raw.customers_pre", "overwrite")

        load_to_postgres(customers_df_tgt_legacy, "legacy.customers", "append")

        
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
        today_date = "20250322"
        
        # Create Spark DataFrame from API data
        csv_file_path = f"gs://meta-morph/{today_date}/sales_{today_date}.csv" 

        df = spark.read.csv(
            csv_file_path,
            header=True,
            sep=",",
            inferSchema=True
        )

        # Rename columns to uppercase
        sales_df = df \
                    .withColumnRenamed("sale_id", "SALE_ID")\
                    .withColumnRenamed("customer_id", "CUSTOMER_ID")\
                    .withColumnRenamed("product_id", "PRODUCT_ID")\
                    .withColumnRenamed("sale_date", "SALE_DATE")\
                    .withColumnRenamed("quantity", "QUANTITY")\
                    .withColumnRenamed("discount", "DISCOUNT")\
                    .withColumnRenamed("shipping_cost", "SHIPPING_COST")\
                    .withColumnRenamed("order_status", "ORDER_STATUS")\
                    .withColumnRenamed("payment_mode", "PAYMENT_MODE")
                    
              
         
        sales_df_tgt =sales_df\
                        .select(
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
        
        sales_df_legacy = sales_df_tgt\
                               .withColumn("DAY_DT", current_date())
        
        sales_df_tgt_legacy = sales_df_legacy\
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

        load_to_postgres(sales_df_tgt_legacy, "legacy.sales", "append")
        
        
        return "Sales ETL process completed successfully."

    except Exception as e:
        log.error("Sales ETL failed: %s", str(e))
        raise AirflowException(f"Sales ETL failed: {str(e)}")

    finally:
            spark.stop()

