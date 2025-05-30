from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres
import logging
from pyspark.sql.functions import col, sum, countDistinct, current_date, row_number, when
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_load_product_performance():
    try:
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_sales - Reads data from 'raw.sales' table
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales")
        SQ_Shortcut_To_sales = SQ_Shortcut_To_sales\
                                .select(
                                    col("ORDER_STATUS"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY"),
                                    col("DISCOUNT")                                 
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_sales' is built....")

        # Processing Node : SQ_Shortcut_To_Products - Reads data from 'raw.products' table
        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                    .select(                                      
                                        col("PRODUCT_ID"),
                                        col("SUPPLIER_ID"),
                                        col("PRODUCT_NAME"),
                                        col("SELLING_PRICE")                                   
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_products' is built....")        
        
       
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

    
    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Supplier_performance ETL failed: {str(e)}")

    finally:
        spark.stop() 