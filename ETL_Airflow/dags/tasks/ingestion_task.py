from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, APIClient, load_to_postgres, DuplicateValidator,read_from_postgres
import logging
from pyspark.sql.functions import col, sum as _sum, countDistinct, current_date, row_number,when
from pyspark.sql.window import Window


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
              
        
        suppliers_df_tgt =suppliers_df\
                            .select(
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )           

        # Validate no duplicates based on SUPPLIER_ID        
        validator = DuplicateValidator()
        validator.validate_no_duplicates(df, key_columns=["SUPPLIER_ID"])


        # Load the cleaned data to PostgreSQL        
        load_to_postgres(suppliers_df_tgt, "raw.suppliers","overwrite")
        
       
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
                        
                
        

        products_df_tgt =products_df\
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

        # Validate no duplicates based on PRODUCTS_ID
        validator = DuplicateValidator()
        validator.validate_no_duplicates(df, key_columns=["PRODUCT_ID"])

        # Load data       
        load_to_postgres(products_df_tgt, "raw.products","overwrite")

        
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
        customers_df =df\
                        .withColumnRenamed("customer_id", "CUSTOMER_ID")\
                        .withColumnRenamed("name", "NAME")\
                        .withColumnRenamed("city", "CITY")\
                        .withColumnRenamed("email", "EMAIL")\
                        .withColumnRenamed("phone_number", "PHONE_NUMBER")
                
        

        customers_df_tgt=customers_df\
                            .select(
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
        load_to_postgres(customers_df_tgt, "raw.customers","overwrite")

        
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

        # Validate no duplicates based on SUPPLIER_ID
        validator = DuplicateValidator()
        validator.validate_no_duplicates(sales_df_tgt, key_columns=["SALE_ID"])

        # Load the cleaned data to PostgreSQL
        load_to_postgres(sales_df_tgt, "raw.sales","overwrite")
        
        
        return "Sales ETL process completed successfully."

    except Exception as e:
        log.error("Sales ETL failed: %s", str(e))
        raise AirflowException(f"Sales ETL failed: {str(e)}")

    finally:
            spark.stop()



@task
def m_load_supplier_performance():
    try:
        spark = init_spark()

        
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales")        
        SQ_Shortcut_To_products = read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_suppliers = read_from_postgres(spark, "raw.suppliers")
       
        sales_filtered = SQ_Shortcut_To_sales.filter(col("ORDER_STATUS") != "CANCELLED")

        
        JNR_Sales_Products = sales_filtered.join(
            SQ_Shortcut_To_products, on="PRODUCT_ID", how="inner"
        )
        JNR_Products_Suppliers = JNR_Sales_Products.join(
            SQ_Shortcut_To_suppliers, on="SUPPLIER_ID", how="inner"
        )

       
        JNR_Products_Suppliers = JNR_Products_Suppliers.withColumn(
            "REVENUE", col("QUANTITY") * col("SELLING_PRICE")
        )

        
        AGG_TRANS_Product_Level = JNR_Products_Suppliers.groupBy(
            "SUPPLIER_ID", "PRODUCT_ID", "PRODUCT_NAME"
        ).agg(
            _sum("REVENUE").alias("agg_product_revenue"),
            _sum("QUANTITY").alias("agg_stock_sold")
        )

       
        AGG_TRANS_Supplier_Level = AGG_TRANS_Product_Level.groupBy("SUPPLIER_ID").agg(
            _sum("agg_product_revenue").alias("agg_total_revenue"),
            countDistinct("PRODUCT_ID").alias("agg_total_products_sold"),
            _sum("agg_stock_sold").alias("agg_total_stock_sold")
        )

       
        windowSpec = Window.partitionBy("SUPPLIER_ID").orderBy(col("agg_product_revenue").desc())
        ranked_df = AGG_TRANS_Product_Level.withColumn("RANK", row_number().over(windowSpec))

        top_selling_product_df = ranked_df.filter(col("RANK") == 1).select(
            "SUPPLIER_ID", col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
        )

        final_result = SQ_Shortcut_To_suppliers.join(
            AGG_TRANS_Supplier_Level, on="SUPPLIER_ID", how="left"
        ).join(
            top_selling_product_df, on="SUPPLIER_ID", how="left"
        ).fillna(0, subset=[
            "agg_total_revenue", "agg_total_products_sold", "agg_total_stock_sold"
        ]).withColumn(
            "TOP_SELLING_PRODUCT",
            when(
                col("TOP_SELLING_PRODUCT").isNull() | (col("TOP_SELLING_PRODUCT") == ""),
                "No Sales"
            ).otherwise(col("TOP_SELLING_PRODUCT"))
        ).withColumn("DAY_DT", current_date())

        Shortcut_To_supplier_performance_tgt = final_result.select(
                                                col("DAY_DT"),
                                                col("SUPPLIER_ID"),
                                                col("SUPPLIER_NAME"),
                                                col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                col("agg_total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                                                col("agg_total_stock_sold").alias("TOTAL_STOCK_SOLD"),
                                                col("TOP_SELLING_PRODUCT")
                                              )

        
        validator = DuplicateValidator()
        validator.validate_no_duplicates(Shortcut_To_supplier_performance_tgt,key_columns=["SUPPLIER_ID", "DAY_DT"] )

      
        load_to_postgres(Shortcut_To_supplier_performance_tgt,"legacy.supplier_performance","append")   

        return "Supplier Performance task finished."

    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()