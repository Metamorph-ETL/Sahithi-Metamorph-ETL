from airflow.decorators import task
from airflow.exceptions import AirflowException
from utils import init_spark, load_to_postgres, DuplicateValidator, read_from_postgres
import logging
from pyspark.sql.functions import col, sum , countDistinct, current_date, row_number, when
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

@task
def m_adhoc_load_supplier():
    try:
    
        # Initialize Spark session
        spark = init_spark()       

        # Processing Node : SQ_Shortcut_To_Suppliers - Reads data from 'raw.suppliers_pre' table
        df = read_from_postgres(spark, "dev_legacy.sales")

        
        load_to_postgres(df, "legacy.sales", "overwrite")   

        return "Supplier Performance task finished."

    except Exception as e:
        log.error(f"ETL task failed: {str(e)}", exc_info=True)
        raise AirflowException(f"Supplier_performance ETL failed: {str(e)}")

    finally:
        spark.stop()     
                                                                                                          
                              
    

