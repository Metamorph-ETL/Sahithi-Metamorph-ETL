from airflow.decorators import task
from airflow.exceptions import AirflowException
from tasks.utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres, fetch_env_schema
import logging
from pyspark.sql.functions import col, sum, current_date, round, when

log = logging.getLogger(__name__)

@task
def m_load_product_performance(env):
    try:
        
        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']

        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales_pre' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, f"{raw}.sales_pre")
        SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales\
                                .select(
                                    col("ORDER_STATUS"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT")                                 
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products_pre' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, f"{raw}.products_pre")
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
                                    SQ_Shortcut_To_Sales.ORDER_STATUS != "Cancelled"
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
                                            .withColumn("REVENUE", col("DISCOUNTED_PRICE") * col("QUANTITY")) \
                                            .withColumn("PROFIT", (col("DISCOUNTED_PRICE") - col("COST_PRICE")) * col("QUANTITY"))\
                                            .withColumn("QUANTITY", when(col("QUANTITY").isNull(), 0).otherwise(col("QUANTITY"))) \
                                            .withColumn("REVENUE", when(col("REVENUE").isNull(), 0).otherwise(col("REVENUE"))) \
                                            .withColumn("PROFIT", when(col("PROFIT").isNull(), 0).otherwise(col("PROFIT")))
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
                                            "AVAILABLE_STOCK", 
                                            (col("STOCK_QUANTITY") - col("TOTAL_QUANTITY_SOLD"))
                                        )\
                                .withColumn(
                                    "STOCK_LEVEL_STATUS", 
                                    when(
                                        col("AVAILABLE_STOCK") < col("REORDER_LEVEL"), 
                                        ("Below Reorder Level")
                                    )\
                                    .otherwise("Sufficient Stock")
                                )\
                                .withColumn("DAY_DT", current_date())
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

        load_to_postgres(Shortcut_To_Product_Performance_Tgt, f"{legacy}.product_performance", "append")   

        return "Product Performance task finished."         
    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Product_performance ETL failed: {str(e)}")      

    finally:
        spark.stop() 