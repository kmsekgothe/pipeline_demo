from pathlib import Path
from src.pipelines.ingestion import ingest
from src.db_utils.ddl import run_ddl
from src.pipelines.transformation import transform
from src.pipelines.load import copy_df_into_table
from src.db_utils.views import create_views
from src.utils.logging import get_logger, log_step
import time

def load_clean(cleaned):
    #in this order
    cols1 =['customer_id',
            'email',
            'full_name',
            'signup_date',
            'country_code',
            'is_active']
    copy_df_into_table(cleaned["customers"],"landing.customers",cols1)
    
    cols2 =["order_id" 
        ,"customer_id" 
        ,"order_ts" 
        ,"status" 
        ,"total_amount"
        ,"currency"]
    copy_df_into_table(cleaned["orders"],"landing.orders",cols2)
    
    cols3 =[ "order_id" 
        ,"line_no"  
        ,"sku" 
        ,"quantity" 
        ,"unit_price"
        ,"category"]
    copy_df_into_table(cleaned["order_items"],"landing.order_items",cols3)
    
def main():
    logger = get_logger("go_pipeline")
    
    pipeline_start = time.perf_counter()
    logger.info("Pipeline started")
    
    with log_step(logger, "Ingest"):
        ingested_data_frames = ingest(Path("files_config.yaml"))

    
    for table_name, data_frame in ingested_data_frames.items():
        print(f"{table_name}: {len(data_frame)} rows")

    with log_step(logger, "Transform"):
        cleaned, rejects = transform(ingested_data_frames)
    
    with log_step(logger, "Load"):
        load_clean(cleaned)
    
    total_duration = time.perf_counter() - pipeline_start
    logger.info(f"Pipeline finished in {total_duration:.2f}s")
    
    create_views()
    
    
if __name__ == "__main__":
    #run_ddl()
    
    main()