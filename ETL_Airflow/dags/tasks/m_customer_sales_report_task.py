from airflow.decorators import task
from airflow.exceptions import AirflowException
from tasks.utils import init_spark, load_to_postgres, DuplicateValidator, fetch_env_schema,read_from_postgres
import logging
from pyspark.sql.functions import col, sum, current_date, round, when, date_sub, month, year, percent_rank, current_timestamp,lit,date_format
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_load_customer_sales_report(env):

    """
    ETL task to load and transform customer sales data and generate a sales report.

    This task:
    1. Extracts sales, product, customer, and supplier performance data.
    2. Transforms the data by filtering, joining, and calculating sales metrics.
    3. Aggregates customer sales to assign loyalty tiers.
    4. Flags top-performing products.
    5. Validates for duplicate records.
    6. Loads the result into a Postgres table.

    Args:
        env (str): Environment name to determine schema configurations.

    Returns:
        str: Success message if the task completes successfully.

    Raises:
        AirflowException: If any error occurs during the ETL process.
    """
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
                                    (col("ORDER_STATUS") != "Cancelled")                              
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
                                how="inner"
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
                                    .withColumn('SALE_MONTH', date_format(col('SALE_DATE'), 'MMMM'))\
                                    .withColumn("SALE_YEAR", year(col("SALE_DATE")))\
                                    .withColumn("PRICE", col("SELLING_PRICE")* (1 - col("DISCOUNT")/100))\
                                    .withColumn("SALE_AMOUNT", 
                                               round(col("QUANTITY") * col("SELLING_PRICE") * 
                                               (1 - col("DISCOUNT")/100), 2))
        log.info(f"Data Frame : 'EXP_Calculate_Metrics' is built....")
             
        # Process the Node : AGG_TRANS_Customer - Calculate the aggregates.
        AGG_TRANS_Customer = EXP_Calculate_Metrics \
            .groupBy("CUSTOMER_ID") \
            .agg(
                sum("SALE_AMOUNT").alias("AGG_SALES_AMOUNT")
            )
        logging.info("Data Frame : 'AGG_TRANS_Customer' is built...")

        quantiles = AGG_TRANS_Customer.approxQuantile(
            "AGG_SALES_AMOUNT",
            [0.5, 0.8], 0.01
        )
        silver_tier = quantiles[0]
        gold_tier = quantiles[1]

      # Process the Node :JNR_CALCULATE_METRICS - Add Loyalty_Tier column
        EXP_Customer_Sales_Report = AGG_TRANS_Customer \
            .withColumn(
                "LOYALTY_TIER",
                when(col("AGG_SALES_AMOUNT") > gold_tier, "GOLD")
                .when(
                    col("AGG_SALES_AMOUNT").between(silver_tier, gold_tier),
                    "SILVER"
                )
                .otherwise("BRONZE")
            )
        logging.info("Data Frame : 'EXP_Customer_Sales_Report' is built...")

        JNR_CALCULATE_METRICS = EXP_Customer_Sales_Report.alias("cust") \
                                .join(
                                    EXP_Calculate_Metrics.alias("metrics"),
                                    col("cust.CUSTOMER_ID") == col("metrics.CUSTOMER_ID"),
                                    how="left"
                                ) \
                                .select(
                                    col("cust.LOYALTY_TIER").alias("LOYALTY_TIER"),
                                    col("cust.AGG_SALES_AMOUNT").alias("AGG_SALES_AMOUNT"),
                                    col("metrics.SALE_ID").alias("SALE_ID"),
                                    col("metrics.CUSTOMER_ID").alias("CUSTOMER_ID"),
                                    col("metrics.PRODUCT_ID").alias("PRODUCT_ID"),
                                    col("metrics.QUANTITY").alias("QUANTITY"),
                                    col("metrics.DISCOUNT").alias("DISCOUNT"),
                                    col("metrics.ORDER_STATUS").alias("ORDER_STATUS"),
                                    col("metrics.PRODUCT_NAME").alias("PRODUCT_NAME"),
                                    col("metrics.CATEGORY").alias("CATEGORY"),
                                    col("metrics.SELLING_PRICE").alias("SELLING_PRICE"),
                                    col("metrics.NAME").alias("NAME"),
                                    col("metrics.CITY").alias("CITY"),
                                    col("metrics.DAY_DT").alias("DAY_DT"),
                                    col("metrics.SALE_DATE").alias("SALE_DATE"),
                                    col("metrics.SALE_MONTH").alias("SALE_MONTH"),
                                    col("metrics.SALE_YEAR").alias("SALE_YEAR"),
                                    round(col("metrics.PRICE"),2).alias("PRICE"),
                                    col("metrics.SALE_AMOUNT").alias("SALE_AMOUNT")
                                )

        log.info(f"Data Frame : 'JNR_CALCULATE_METRICS' is built....")
        
       # Processing Node: SQ_Shortcut_To_Top_Selling_Products - Select relevant top performers
        SQ_Shortcut_To_Top_Selling_Products = SQ_Shortcut_To_Supplier_Performance\
                                                .select(
                                                    col("TOP_SELLING_PRODUCT"),
                                                    lit("true").alias("TOP_PERFORMER")
                                                )
        # Processing Node: JNR_With_Top_Performer - Join top performer info with main data
        JNR_With_Top_Performer = JNR_CALCULATE_METRICS\
                                    .join(
                                        SQ_Shortcut_To_Top_Selling_Products,
                                       JNR_CALCULATE_METRICS.PRODUCT_NAME == SQ_Shortcut_To_Top_Selling_Products.TOP_SELLING_PRODUCT,
                                        how="left"
                                    )\
                                    .select(
                                          JNR_CALCULATE_METRICS.DAY_DT,
                                           JNR_CALCULATE_METRICS.CUSTOMER_ID,
                                           JNR_CALCULATE_METRICS.NAME.alias("CUSTOMER_NAME"),
                                           JNR_CALCULATE_METRICS.SALE_ID,
                                           JNR_CALCULATE_METRICS.CITY,
                                           JNR_CALCULATE_METRICS.PRODUCT_NAME,
                                           JNR_CALCULATE_METRICS.CATEGORY,
                                           JNR_CALCULATE_METRICS.SALE_DATE,
                                           JNR_CALCULATE_METRICS.SALE_MONTH,
                                           JNR_CALCULATE_METRICS.SALE_YEAR,
                                           JNR_CALCULATE_METRICS.QUANTITY,
                                           JNR_CALCULATE_METRICS.PRICE,
                                           JNR_CALCULATE_METRICS.SALE_AMOUNT,
                                           JNR_CALCULATE_METRICS.LOYALTY_TIER,
                                           SQ_Shortcut_To_Top_Selling_Products.TOP_PERFORMER
                                   )\
                                    .withColumn(
                                            "TOP_PERFORMER",
                                            when(col("TOP_PERFORMER").isNull(), "false").otherwise(col("TOP_PERFORMER"))
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

        load_to_postgres(Shortcut_To_CSR_Tgt, f"{legacy}.customer_sales_report", "append")   

        return "Customer Sales Report task finished."         
    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Customer Sales Report ETL failed: {str(e)}")

    finally:
        spark.stop()