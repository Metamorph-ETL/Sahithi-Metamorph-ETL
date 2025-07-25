from airflow.decorators import task
from airflow.exceptions import AirflowException
from datetime import datetime
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres
from pyspark.sql.functions import col, sum , current_date, row_number, when,round
from pyspark.sql.window import Window
import logging

log = logging.getLogger(__name__)

@task
def m_ingest_data_into_sales():
    try:
        # Start Spark session
       
        spark = init_spark()

        # API Client to fetch data
        log.info("Fetching sales data from GCS bucket...")            
        today_date = "20250707"
        
        # Create Spark DataFrame from API data
        csv_file_path = f"gs://meta-morph-flow/{today_date}/sales_{today_date}.csv" 

        df = spark.read.csv(
            csv_file_path,
            header=True,
            sep=",",
            inferSchema=True
        )
        cols = [c.strip().upper().replace(" ", "_") for c in df.columns]
        sales_df = df.toDF(*cols)
         
        sales_df_tgt = sales_df\
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
        
        sales_legacy_df = sales_df_tgt\
                               .withColumn("DAY_DT", current_date() - 1)
        
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



@task
def m_load_supplier_performance():
    try:
    
        # Initialize Spark session
        spark = init_spark()

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales_pre")
        SQ_Shortcut_To_sales = SQ_Shortcut_To_sales\
                                .select(
                                    col("ORDER_STATUS"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT")                                 
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products_pre")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("SUPPLIER_ID"),
                                        col("PRODUCT_NAME"),
                                        col("SELLING_PRICE")                                   
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_products' is built....")

        # Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers' table
        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers_pre")
        SQ_Shortcut_To_Suppliers =  SQ_Shortcut_To_Suppliers\
                                    .select(
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME")
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_suppliers' is built....")
        
        # Processing Node : FIL_Cancelled_Sales - Filters out cancelled orders
        FIL_Cancelled_Sales = SQ_Shortcut_To_sales\
                                .filter(
                                    SQ_Shortcut_To_sales.ORDER_STATUS != "CANCELLED"
                                )
        log.info(f"Data Frame : 'FIL_Cancelled_Sales' is built....")
        
        # Processing Node : JNR_Sales_Products - Joins sales data with product data on PRODUCT_ID
        JNR_Sales_Products = FIL_Cancelled_Sales\
                                .join( 
                                    SQ_Shortcut_To_Products, 
                                    on="PRODUCT_ID",
                                    how="inner"
                                )\
                                .select(
                                    SQ_Shortcut_To_sales.QUANTITY,
                                    SQ_Shortcut_To_sales.DISCOUNT,
                                    SQ_Shortcut_To_Products.PRODUCT_ID, 
                                    SQ_Shortcut_To_Products.SUPPLIER_ID,
                                    SQ_Shortcut_To_Products.PRODUCT_NAME,
                                    SQ_Shortcut_To_Products.SELLING_PRICE
                                )       
        log.info(f"Data Frame : 'JNR_Sales_Products' is built....")
        
        # Processing Node : JNR_Products_Suppliers - Joins product-sales data with supplier data on SUPPLIER_ID
        JNR_Products_Suppliers = JNR_Sales_Products\
                                    .join(
                                        SQ_Shortcut_To_Suppliers,
                                        on="SUPPLIER_ID",
                                        how="inner"
                                    )\
                                    .select(
                                        JNR_Sales_Products.PRODUCT_ID,
                                        JNR_Sales_Products.PRODUCT_NAME,
                                        JNR_Sales_Products.QUANTITY,
                                        JNR_Sales_Products.SELLING_PRICE,
                                        JNR_Sales_Products.DISCOUNT,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_ID,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_NAME  
                                     )\
                                    .withColumn(
                                        "REVENUE",  (col("SELLING_PRICE") - (col("SELLING_PRICE") * col("DISCOUNT") / 100)) * col("QUANTITY")
                                    )        
        log.info(f"Data Frame : 'JNR_Products_Suppliers' is built....")                             
                                   
        # Processing Node : AGG_TRANS_Product_Level - Aggregates data at the product level per supplier
        AGG_TRANS_Product_Level = JNR_Products_Suppliers\
                                    .groupBy(
                                        ["SUPPLIER_ID", "PRODUCT_ID", "PRODUCT_NAME"]
                                    )\
                                    .agg(
                                        sum("REVENUE").alias("agg_product_revenue"),
                                        sum("QUANTITY").alias("agg_stock_sold")
                                   )
        log.info(f"Data Frame : 'AGG_TRANS_Product_Level' is built....")

        # Processing Node - AGG_TRANS_Product_Level - Aggregates 'valid_product' column based on stock sold > 0
        AGG_TRANS_Product_Level = AGG_TRANS_Product_Level\
                                    .withColumn("valid_product", when(col("agg_stock_sold") > 0, 1).otherwise(0))
        log.info(f"Data Frame : 'AGG_TRANS_Product_Level' is built....")
            
        # Processing Node : AGG_TRANS_Supplier_Level - Aggregates data at the supplier level
        AGG_TRANS_Supplier_Level = AGG_TRANS_Product_Level\
                                    .groupBy(
                                        "SUPPLIER_ID"
                                    )\
                                    .agg(
                                        sum("agg_product_revenue").alias("agg_total_revenue"),
                                        sum("valid_product").alias("agg_total_products_sold"),
                                        sum("agg_stock_sold").alias("agg_total_stock_sold")
                                    )
        log.info(f"Data Frame : 'AGG_TRANS_Supplier_Level' is built....")
       
        # Processing Node : RNK_Suppliers_df - Ranks products per supplier based on revenue
        windowSpec = Window.partitionBy("SUPPLIER_ID").orderBy(col("agg_product_revenue").desc())
        RNK_Suppliers_df = AGG_TRANS_Product_Level.withColumn("RANK", row_number().over(windowSpec))
        log.info(f"Data Frame : 'RNK_Suppliers_df' is built....")

        # Processing Node : Top_Selling_Product_df - Filters to get the top selling product per supplier
        Top_Selling_Product_df = RNK_Suppliers_df\
                                    .filter(col("RANK") == 1)\
                                    .select(
                                        col("SUPPLIER_ID"),
                                        col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
                                    )
        log.info(f"Data Frame : 'Top_Selling_Product_df' is built....")

        # Processing Node : JNR_Supplier_Agg_Level - Combines all supplier metrics 
        JNR_Supplier_Agg_Level = SQ_Shortcut_To_Suppliers\
                                    .join(
                                        AGG_TRANS_Supplier_Level,
                                        on="SUPPLIER_ID",
                                        how="left"         
                                    )\
                                    .select(
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_ID,
                                        SQ_Shortcut_To_Suppliers.SUPPLIER_NAME,
                                        AGG_TRANS_Supplier_Level.agg_total_revenue,
                                        AGG_TRANS_Supplier_Level.agg_total_products_sold,
                                        AGG_TRANS_Supplier_Level.agg_total_stock_sold
                                    )
        log.info(f"Data Frame : 'JNR_Supplier_Agg_Level' is built....")

        # Processing Node : JNR_Supplier_Agg_Top_Selling_Product - Combines all supplier_Agg_level metrics and top product
        JNR_Supplier_Agg_Top_Selling_Product = JNR_Supplier_Agg_Level\
                                                    .join(
                                                        Top_Selling_Product_df,
                                                        on="SUPPLIER_ID", 
                                                        how="left"
                                                    )\
                                                    .select(
                                                        JNR_Supplier_Agg_Level.SUPPLIER_ID,
                                                        JNR_Supplier_Agg_Level.SUPPLIER_NAME,
                                                        JNR_Supplier_Agg_Level.agg_total_revenue,
                                                        JNR_Supplier_Agg_Level.agg_total_products_sold,
                                                        JNR_Supplier_Agg_Level.agg_total_stock_sold,
                                                        Top_Selling_Product_df.TOP_SELLING_PRODUCT
                                                    )\
                                                    .fillna(
                                                        0, subset=["agg_total_revenue", "agg_total_products_sold", "agg_total_stock_sold"]
                                                    )\
                                                    .withColumn(
                                                        "TOP_SELLING_PRODUCT",
                                                        when(
                                                            col("TOP_SELLING_PRODUCT").isNull() | (col("TOP_SELLING_PRODUCT") == ""),
                                                            "No Sales"
                                                        )\
                                                        .otherwise(
                                                            col("TOP_SELLING_PRODUCT")
                                                        )
                                                    )\
                                                    .withColumn("DAY_DT", current_date() - 1)              
        log.info(f"Data Frame : 'JNR_Supplier_Agg_Top_Selling_Product' is built....")

        # Processing Node : Shortcut_To_Supplier_Performance_Tgt - Final selection for loading to target
        Shortcut_To_Supplier_Performance_Tgt = JNR_Supplier_Agg_Top_Selling_Product\
                                                    .select(
                                                        col("DAY_DT"),
                                                        col("SUPPLIER_ID"),
                                                        col("SUPPLIER_NAME"),
                                                        col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                        col("agg_total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                                                        col("agg_total_stock_sold").alias("TOTAL_STOCK_SOLD"),
                                                        col("TOP_SELLING_PRODUCT")
                                                    )
        log.info(f"Data Frame : 'Shortcut_To_Supplier_Performance_Tgt' is built....")

        validator = DuplicateValidator()
        validator.validate_no_duplicates(Shortcut_To_Supplier_Performance_Tgt,key_columns=["SUPPLIER_ID", "DAY_DT"] )

      
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt, "legacy.supplier_performance", "append")   

        return "Supplier Performance task finished."

    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Supplier_performance ETL failed: {str(e)}")

    finally:
        spark.stop()     
                                                                                                          



@task
def m_load_product_performance():
    try:
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales_pre")
        SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales\
                                .select(
                                    col("ORDER_STATUS"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT")                                 
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products_pre")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("COST_PRICE"),
                                        col("PRODUCT_NAME"),
                                        col("SELLING_PRICE"),
                                        col("CATEGORY"),
                                        col("STOCK_QUANTITY"),
                                        col("REORDER_LEVEL")                           
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built....")        
        
       
        # Processing Node : FIL_Cancelled_Sales - Filters out cancelled orders
        FIL_Cancelled_Sales = SQ_Shortcut_To_Sales\
                                .filter(
                                    SQ_Shortcut_To_Sales.ORDER_STATUS != "CANCELLED"
                                )
        log.info(f"Data Frame : 'FIL_Cancelled_Sales' is built....")

         
        # Processing Node : JNR_Sales_Products - Joins sales data with product data on PRODUCT_ID
        JNR_Sales_Products = SQ_Shortcut_To_Products\
                                .join( 
                                    FIL_Cancelled_Sales, 
                                    on="PRODUCT_ID",
                                    how="left"
                                )\
                                .select(
                                    SQ_Shortcut_To_Products.PRODUCT_ID,  
                                    SQ_Shortcut_To_Products.COST_PRICE,
                                    SQ_Shortcut_To_Products.PRODUCT_NAME,
                                    SQ_Shortcut_To_Products.SELLING_PRICE,
                                    SQ_Shortcut_To_Products.CATEGORY,
                                    SQ_Shortcut_To_Products.STOCK_QUANTITY, 
                                    SQ_Shortcut_To_Products.REORDER_LEVEL,
                                    FIL_Cancelled_Sales.QUANTITY,
                                    FIL_Cancelled_Sales.DISCOUNT
                                )       
        log.info(f"Data Frame : 'JNR_Sales_Products' is built....")

        # Processing Node :  EXP_Calculate_Product_Metrics -  Derives revenue, profit, and discounted price per product row
        EXP_Calculate_Product_Metrics = JNR_Sales_Products\
                                            .withColumn("DISCOUNTED_PRICE", col("SELLING_PRICE") * (1 - col("DISCOUNT") / 100)) \
                                            .withColumn("REVENUE", round(col("DISCOUNTED_PRICE") * col("QUANTITY"))) \
                                            .withColumn("PROFIT", (col("SELLING_PRICE") - col("COST_PRICE")) * col("QUANTITY"))\
                                            .fillna(0, ["QUANTITY", "REVENUE", "PROFIT"])
        log.info(f"Data Frame : 'EXP_Calculate_Product_Metrics' is built....")                              
            
       # Processing Node :  AGG_TRANS_Product_Level -  Aggregate product metrics
        AGG_TRANS_Product_Level = EXP_Calculate_Product_Metrics\
                                    .groupBy(
                                        ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "STOCK_QUANTITY", "REORDER_LEVEL"]
                                    )\
                                    .agg(
                                        round(sum("REVENUE"), 2).alias("TOTAL_SALES_AMOUNT"),
                                        sum("QUANTITY").alias("TOTAL_QUANTITY_SOLD"),
                                        round(sum("PROFIT"), 2).alias("PROFIT")
                                    )
        log.info(f"Data Frame : 'AGG_TRANS_Product_Level' is built....")
        
        # Processing Node :  EXP_Final_Transform  Adds derived metrics like avg price,stock status and date 
        EXP_Final_Transform = AGG_TRANS_Product_Level \
                                 .withColumn(
                                        "AVG_SALE_PRICE", 
                                        when(
                                            col("TOTAL_QUANTITY_SOLD") > 0,
                                            round(col("TOTAL_SALES_AMOUNT") / col("TOTAL_QUANTITY_SOLD"), 2)
                                        )\
                                        .otherwise(0.0)  
                                    ) \
                                .withColumn(
                                    "STOCK_LEVEL_STATUS", 
                                    when(
                                        col("STOCK_QUANTITY") <= col("REORDER_LEVEL"), 
                                        ("BELOW_REORDER_LEVEL")
                                    )\
                                    .otherwise("STOCK_OK")
                                )\
                                .withColumn("DAY_DT", current_date() - 1)
        log.info(f"Data Frame : 'EXP_Final_Transform' is built....")

       
        
        # Processing Node : Shortcut_To_Prodct_Performance_Tgt - Final selection for loading to target
        Shortcut_To_Product_Performance_Tgt = EXP_Final_Transform\
                                                    .select(
                                                        col("DAY_DT"),
                                                        col("PRODUCT_ID"),
                                                        col("PRODUCT_NAME"),
                                                        col("TOTAL_SALES_AMOUNT"),
                                                        col("TOTAL_QUANTITY_SOLD"),
                                                        col("AVG_SALE_PRICE"),
                                                        col("STOCK_QUANTITY"),
                                                        col("REORDER_LEVEL"),
                                                        col("STOCK_LEVEL_STATUS"),
                                                        col("PROFIT"),
                                                        col("CATEGORY")
                                                    )
        log.info(f"Data Frame : 'Shortcut_To_Product_Performance_Tgt' is built....")


        # Validate and load data
        validator = DuplicateValidator()
        validator.validate_no_duplicates(Shortcut_To_Product_Performance_Tgt, key_columns=["PRODUCT_ID", "DAY_DT"]) 

        load_to_postgres(Shortcut_To_Product_Performance_Tgt, "legacy.product_performance", "append")   

        return "Product Performance task finished."         
    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Product_performance ETL failed: {str(e)}")

    finally:
        spark.stop() 
    