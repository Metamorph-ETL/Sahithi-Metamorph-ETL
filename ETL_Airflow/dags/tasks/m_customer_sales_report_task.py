from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres,fetch_env_schema
import logging
from pyspark.sql.functions import col, sum, current_date, round, when, date_sub, month, year, percent_rank, current_timestamp,lit
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_load_customer_sales_report(env):
    try:

        raw = fetch_env_schema(env)['raw']
        legacy = fetch_env_schema(env)['legacy']
        
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales_pre' table
        SQ_Shortcut_To_Sales = read_from_postgres(spark, f"{raw}.sales_pre")
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

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products_pre' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, f"{raw}.products_pre")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products\
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("PRODUCT_NAME"),
                                        col("CATEGORY"),
                                        col("SELLING_PRICE")                           
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built....")

        # Processing Node : SQ_Shortcut_To_Customers - Reads data from 'raw.customers_pre' table
        SQ_Shortcut_To_Customers = read_from_postgres(spark, f"{raw}.customers_pre")
        SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers\
                                    .select(
                                        col("CUSTOMER_ID"),
                                        col("NAME"),
                                        col("CITY")
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_Customers' is built....")

        # Processing Node : SQ_Shortcut_To_Supplier_Performance - Read from 'legacy.supplier_performance'
        SQ_Shortcut_To_Supplier_Performance = read_from_postgres(spark, f"{legacy}.supplier_performance")\
                                                .select(
                                                    col("TOP_SELLING_PRODUCT"), 
                                                    col("DAY_DT")
                                                )\
                                                .filter(
                                                    col("DAY_DT") == current_date()
                                                )

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
                                )\
                                .select(
                                        FIL_Cancelled_Sales.SALE_ID,
                                        FIL_Cancelled_Sales.CUSTOMER_ID,
                                        FIL_Cancelled_Sales.PRODUCT_ID,
                                        FIL_Cancelled_Sales.QUANTITY,
                                        FIL_Cancelled_Sales.DISCOUNT,
                                        FIL_Cancelled_Sales.SALE_DATE,
                                        FIL_Cancelled_Sales.ORDER_STATUS,
                                        SQ_Shortcut_To_Products.PRODUCT_NAME,
                                        SQ_Shortcut_To_Products.CATEGORY,
                                        SQ_Shortcut_To_Products.SELLING_PRICE 
                                )
        
        log.info(f"Data Frame : 'JNR_Sales_Products' is built....")

        # Processing Node : JNR_All_Data - Joins with customer data
        JNR_All_Data = JNR_Sales_Products\
                            .join(
                                SQ_Shortcut_To_Customers,
                                on="CUSTOMER_ID",
                                how="left"
                            )\
                            .select(
                                    JNR_Sales_Products.SALE_ID,
                                    JNR_Sales_Products.CUSTOMER_ID,
                                    JNR_Sales_Products.PRODUCT_ID,
                                    JNR_Sales_Products.QUANTITY,
                                    JNR_Sales_Products.DISCOUNT,
                                    JNR_Sales_Products.SALE_DATE,
                                    JNR_Sales_Products.ORDER_STATUS,
                                    JNR_Sales_Products.PRODUCT_NAME,
                                    JNR_Sales_Products.CATEGORY,
                                    JNR_Sales_Products.SELLING_PRICE,
                                    SQ_Shortcut_To_Customers.NAME,
                                    SQ_Shortcut_To_Customers.CITY                                              
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

       # Processing Node: SQ_Shortcut_To_Top_Selling_Products - Select relevant top performers
        SQ_Shortcut_To_Top_Selling_Products = SQ_Shortcut_To_Supplier_Performance\
                                                .select(
                                                    col("TOP_SELLING_PRODUCT"),
                                                    lit("Y").alias("TOP_PERFORMER")
                                                )

        # Processing Node: JNR_With_Top_Performer - Join top performer info with main data
        JNR_With_Top_Performer = JNR_With_Loyalty\
                                    .join(
                                        SQ_Shortcut_To_Top_Selling_Products,
                                        JNR_With_Loyalty.PRODUCT_NAME == SQ_Shortcut_To_Top_Selling_Products.TOP_SELLING_PRODUCT,
                                        how="left"
                                    )\
                                    .select(
                                            JNR_With_Loyalty.DAY_DT,
                                            JNR_With_Loyalty.CUSTOMER_ID,
                                            JNR_With_Loyalty.NAME.alias("CUSTOMER_NAME"),
                                            JNR_With_Loyalty.SALE_ID,
                                            JNR_With_Loyalty.CITY,
                                            JNR_With_Loyalty.PRODUCT_NAME,
                                            JNR_With_Loyalty.CATEGORY,
                                            JNR_With_Loyalty.SALE_DATE,
                                            JNR_With_Loyalty.SALE_MONTH,
                                            JNR_With_Loyalty.SALE_YEAR,
                                            JNR_With_Loyalty.QUANTITY,
                                            JNR_With_Loyalty.PRICE,
                                            JNR_With_Loyalty.SALE_AMOUNT,
                                            JNR_With_Loyalty.LOYALTY_TIER,
                                            SQ_Shortcut_To_Top_Selling_Products.TOP_PERFORMER
                                   )\
                                    .withColumn(
                                            "TOP_PERFORMER",
                                            when(col("TOP_PERFORMER").isNull(), "N").otherwise(col("TOP_PERFORMER"))
                                   )\
                                   .withColumn("LOAD_TSTMP", current_timestamp())
        log.info(f"Data Frame : 'JNR_With_Top_Performer' is built....")


        # Processing Node : Shortcut_To_CSR_Tgt - Final selection for target
        Shortcut_To_CSR_Tgt = JNR_With_Top_Performer\
                                   .select(
                                        col("DAY_DT"),
                                        col("CUSTOMER_ID"),
                                        col("CUSTOMER_NAME"),
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

        load_to_postgres(Shortcut_To_CSR_Tgt,f"{legacy}.customer_sales_report", "append")   

        return "Customer Sales Report task finished."         
    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Customer Sales Report ETL failed: {str(e)}")

    finally:
        spark.stop()