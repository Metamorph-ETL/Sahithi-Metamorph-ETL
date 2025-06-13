from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres
import logging
from pyspark.sql.functions import col, sum, current_date, round, when, date_sub, month, year, percent_rank, current_timestamp,lit
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_load_customer_sales_report():
    try:
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, "raw.sales")
        SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales\
                                .select(
                                    col("SALE_ID"),
                                    col("CUSTOMER_ID"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT"),
                                    col("SALE_DATE"),
                                    col("ORDER_STATUS")                                
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("SELLING_PRICE")                           
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built....")

        # Processing Node : SQ_Shortcut_To_Customers - Reads data from 'raw.customers' table
        SQ_Shortcut_To_Customers = read_from_postgres(spark, "raw.customers")
        SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers \
                                    .select(
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY")
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_Customers' is built....")

        # Processing Node : SQ_Shortcut_To_Supplier_Performance - Read from 'legacy.supplier_performance'
        SQ_Shortcut_To_Supplier_Performance = read_from_postgres(spark, "legacy.supplier_performance")\
                                                .select("TOP_SELLING_PRODUCT")
        log.info(f"Data Frame : 'SQ_Shortcut_To_Supplier_Performance' is built....")

        # Processing Node : FIL_Cancelled_Sales - Filters out cancelled orders
        FIL_Cancelled_Sales = SQ_Shortcut_To_Sales\
                                .filter(
                                    (col("ORDER_STATUS") != "CANCELLED")                              
                                )
        log.info(f"Data Frame : 'FIL_Cancelled_Sales' is built....")

        # Processing Node : JNR_Sales_Products - Joins sales data with product data
        JNR_Sales_Products = FIL_Cancelled_Sales\
                                .join(
                                    SQ_Shortcut_To_Products,
                                    on="PRODUCT_ID",
                                    how="left"
                                )
        log.info(f"Data Frame : 'JNR_Sales_Products' is built....")

        # Processing Node : JNR_All_Data - Joins with customer data
        JNR_All_Data = JNR_Sales_Products\
                            .join(
                                SQ_Shortcut_To_Customers,
                                on="CUSTOMER_ID",
                                how="left"
                            )
        log.info(f"Data Frame : 'JNR_All_Data' is built....")

        # Processing Node : EXP_Calculate_Metrics - Calculates derived fields
        EXP_Calculate_Metrics = JNR_All_Data\
                                    .withColumn("DAY_DT", current_date())\
                                    .withColumn("SALE_DATE", 
                                            when(
                                                col("SALE_DATE").isNull(), 
                                                date_sub(current_date(),
                                                 1)
                                            )\
                                          .otherwise(
                                              col("SALE_DATE")
                                              )
                                            )\
                                    .withColumn("SALE_MONTH", month(col("SALE_DATE")))\
                                    .withColumn("SALE_YEAR", year(col("SALE_DATE")))\
                                    .withColumn("PRICE", round(col("SELLING_PRICE"), 2))\
                                    .withColumn("SALE_AMOUNT", 
                                               round(col("QUANTITY") * col("SELLING_PRICE") * 
                                               (1 - col("DISCOUNT")/100), 2))
        log.info(f"Data Frame : 'EXP_Calculate_Metrics' is built....")

             
        # Processing Node : JNR_With_Loyalty - Join loyalty tier back to main data
        rank_window = Window.orderBy(col("SALE_AMOUNT").desc())
        JNR_With_Loyalty = EXP_Calculate_Metrics\
                                .withColumn("PURCHASE_RANK", percent_rank().over(rank_window))\
                                .withColumn("LOYALTY_TIER",
                                    when(
                                        col("PURCHASE_RANK") <= 0.2, 
                                        "Gold"
                                    )\
                                    .when(
                                        col("PURCHASE_RANK") <= 0.5, 
                                        "Silver"
                                    )\
                                     .otherwise("Bronze")
                                )
        log.info(f"Data Frame : 'JNR_With_Loyalty' is built....")

        
        # Processing Node : Top_Performer_df - Based on supplier performance presence
        Top_Performer_df = SQ_Shortcut_To_Supplier_Performance\
                            .filter(~(col("TOP_SELLING_PRODUCT") == "No Sales"))\
                            .select(col("TOP_SELLING_PRODUCT"))\
                            .withColumn("TOP_PERFORMER", lit("Y"))
        log.info(f"Data Frame : 'Top_Performer_df' is built using supplier performance....")     
         
        
        # Processing Node : JNR_With_Top_Performer -  Join top performer info with main data
        JNR_With_Top_Performer = JNR_With_Loyalty\
                                    .join(
                                        Top_Performer_df,
                                         on=JNR_With_Loyalty["PRODUCT_NAME"] == Top_Performer_df["TOP_SELLING_PRODUCT"],
                                        how="left"
                                    )\
                                    .withColumn("TOP_PERFORMER", when(col("TOP_PERFORMER").isNull(), "N").otherwise(col("TOP_PERFORMER")))     
        log.info(f"Data Frame : 'JNR_With_Top_Performer' is built....")           


        # Processing Node : Shortcut_To_CSR_Tgt - Final selection for target
        Shortcut_To_CSR_Tgt = JNR_With_Top_Performer\
                                 .withColumn("LOAD_TSTMP", current_timestamp())\
                                 .select(
                                        col("DAY_DT"),
                                        col("CUSTOMER_ID"),
                                        col("NAME").alias("CUSTOMER_NAME"),
                                        col("SALE_ID"),
                                        col("CITY"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("SALE_DATE"),
                                        col("SALE_MONTH"),
                                        col("SALE_YEAR"),
                                        col("QUANTITY"),
                                        col("PRICE"),
                                        col("SALE_AMOUNT"),
                                        col("TOP_PERFORMER"),
                                        col("LOYALTY_TIER"),
                                        col("LOAD_TSTMP")
                                    )             
        log.info(f"Data Frame : 'Shortcut_To_CSR_Tgt' is built....")

        # Validate and load data
        validator = DuplicateValidator()
        validator.validate_no_duplicates(Shortcut_To_CSR_Tgt, key_columns=["SALE_ID", "DAY_DT"]) 

        load_to_postgres(Shortcut_To_CSR_Tgt, "legacy.customer_sales_report", "append")   

        return "Customer Sales Report task finished."         
    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Customer Sales Report ETL failed: {str(e)}")

    finally:
        spark.stop()