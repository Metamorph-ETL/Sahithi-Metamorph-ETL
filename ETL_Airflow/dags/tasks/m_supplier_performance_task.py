from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark,load_to_postgres,DuplicateValidator,read_from_postgres
import logging
from pyspark.sql.functions import col, sum as _sum  , countDistinct, current_date, row_number,when
from pyspark.sql.window import Window

log = logging.getLogger(__name__)


@task
def m_load_supplier_performance():
    try:
        spark = init_spark()

        #processing node SQ_Shortcut_To_sales -reads data from sales table
        SQ_Shortcut_To_sales = read_from_postgres(spark, "raw.sales")
        SQ_Shortcut_To_sales = SQ_Shortcut_To_sales\
                                 .select(
                                    col("ORDER_STATUS"),
                                    col("PRODUCT_ID"),
                                    col("QUANTITY")                                  
                                )        
        log.info(f"Data Frame : 'SQ_Shortcut_To_sales' is built....")

        SQ_Shortcut_To_Products = read_from_postgres(spark, "raw.products")
        SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                     .select(                                      
                                      col("PRODUCT_ID"),
                                      col("SUPPLIER_ID"),
                                      col("PRODUCT_NAME"),
                                      col("SELLING_PRICE")                                   
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_products' is built....")

        SQ_Shortcut_To_Suppliers = read_from_postgres(spark, "raw.suppliers")
        SQ_Shortcut_To_Suppliers =  SQ_Shortcut_To_Suppliers\
                                    .select(
                                        col("SUPPLIER_ID"),
                                        col("SUPPLIER_NAME")
                                    )
        log.info(f"Data Frame : 'SQ_Shortcut_To_suppliers' is built....")
       
        FIL_Cancelled_Sales = SQ_Shortcut_To_sales.filter(col("ORDER_STATUS") != "CANCELLED")

        
        JNR_Sales_Products = FIL_Cancelled_Sales.select(
            "PRODUCT_ID", "QUANTITY"
        ).join(
        SQ_Shortcut_To_Products.select("PRODUCT_ID", "SUPPLIER_ID", "PRODUCT_NAME"),
            on="PRODUCT_ID",
            how="inner"
        )

        # Join with suppliers with selected columns
        JNR_Products_Suppliers = JNR_Sales_Products.select(
            "PRODUCT_ID", "PRODUCT_NAME", "SUPPLIER_ID", "QUANTITY"
        ).join(
            SQ_Shortcut_To_Suppliers.select("SUPPLIER_ID", "SUPPLIER_NAME"),
            on="SUPPLIER_ID",
            how="inner"
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
        RNK_Suppliers_df = AGG_TRANS_Product_Level.withColumn("RANK", row_number().over(windowSpec))

        Top_Selling_Product_df = RNK_Suppliers_df.filter(col("RANK") == 1).select(
            "SUPPLIER_ID", col("PRODUCT_NAME").alias("TOP_SELLING_PRODUCT")
        )

        JNR_Supplier_Aggregates_And_Top_Product = SQ_Shortcut_To_Suppliers.join(
            AGG_TRANS_Supplier_Level, on="SUPPLIER_ID", how="left"
        ).join(
            Top_Selling_Product_df, on="SUPPLIER_ID", how="left"
        ).fillna(0, subset=[
            "agg_total_revenue", "agg_total_products_sold", "agg_total_stock_sold"
        ]).withColumn(
            "TOP_SELLING_PRODUCT",
            when(
                col("TOP_SELLING_PRODUCT").isNull() | (col("TOP_SELLING_PRODUCT") == ""),
                "No Sales"
            ).otherwise(col("TOP_SELLING_PRODUCT"))
        ).withColumn("DAY_DT", current_date())

        Shortcut_To_Supplier_Performance_Tgt = JNR_Supplier_Aggregates_And_Top_Product.select(
                                                col("DAY_DT"),
                                                col("SUPPLIER_ID"),
                                                col("SUPPLIER_NAME"),
                                                col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                col("agg_total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                                                col("agg_total_stock_sold").alias("TOTAL_STOCK_SOLD"),
                                                col("TOP_SELLING_PRODUCT")
                                              )

        
        validator = DuplicateValidator()
        validator.validate_no_duplicates(Shortcut_To_Supplier_Performance_Tgt,key_columns=["SUPPLIER_ID", "DAY_DT"] )

      
        load_to_postgres(Shortcut_To_Supplier_Performance_Tgt,"legacy.supplier_performance","append")   

        return "Supplier Performance task finished."

    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Supplier_performance ETL failed: {str(e)}")

    finally:
        spark.stop()