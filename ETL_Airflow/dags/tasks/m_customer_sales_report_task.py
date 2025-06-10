from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres
import logging
from pyspark.sql.functions import col, sum, current_date, round, when, lit, date_sub, month, year, rank, percent_rank,current_timestamp
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_load_customer_sales_report():
    try:
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales")
        SQ_Shortcut_To_sales = SQ_Shortcut_To_sales\
                                .select(
                                    col("SALE_ID"),
                                    col("CUSTOMER_ID"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT"),
                                    col("SALE_DATE"),
                                    col("ORDER_STATUS")                                
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("SELLING_PRICE")                           
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_products' is built....")

        # Processing Node : SQ_Shortcut_To_Customers - Reads data from 'raw.customers' table
        SQ_Shortcut_To_Customers = read_from_postgres(spark, "raw.customers")
        SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers \
                                    .select(
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY")
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_customers' is built....")

        # Processing Node : FIL_Cancelled_Sales - Filters out cancelled orders
        FIL_Cancelled_Sales = SQ_Shortcut_To_sales\
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

        # Processing Node : AGG_Customer_Spending - Calculate customer spending for loyalty tier
        AGG_Customer_Spending = EXP_Calculate_Metrics\
                                    .groupBy("CUSTOMER_ID")\
                                    .agg(sum("SALE_AMOUNT").alias("TOTAL_SPENDING"))
        
        # Processing Node : Add percent rank for loyalty tier calculation
        windowSpec = Window.orderBy(col("TOTAL_SPENDING").desc())
        AGG_Customer_Spending = AGG_Customer_Spending\
                                    .withColumn("SPENDING_RANK", percent_rank().over(windowSpec))
        log.info(f"Data Frame : 'AGG_Customer_Spending' is built....")

        # Processing Node : JNR_With_Loyalty - Join loyalty tier back to main data
        JNR_With_Loyalty = EXP_Calculate_Metrics\
                                .join(
                                    AGG_Customer_Spending,
                                    on="CUSTOMER_ID",
                                    how="left"
                                )\
                                .withColumn("LOYALTY_TIER",
                                    when(
                                        col("SPENDING_RANK") <= 0.2, 
                                        "Gold"
                                    )\
                                    .when(
                                        col("SPENDING_RANK") <= 0.5, 
                                        "Silver"
                                    )\
                                     .otherwise("Bronze")
                                )
        log.info(f"Data Frame : 'JNR_With_Loyalty' is built....")

        # Processing Node : EXP_Final_Transform - Final transformations
        EXP_Final_Transform = JNR_With_Loyalty\
                                .withColumn("TOP_PERFORMER", 
                                    when(
                                        col("SALE_AMOUNT") > lit(1000), 
                                        True
                                    )
                                     .otherwise(False)  
                                )\
                                .withColumn("LOAD_TSTMP", current_timestamp())
        log.info(f"Data Frame : 'EXP_Final_Transform' is built....")

        # Processing Node : Shortcut_To_CSR_Tgt - Final selection for target
        Shortcut_To_CSR_Tgt = EXP_Final_Transform\
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