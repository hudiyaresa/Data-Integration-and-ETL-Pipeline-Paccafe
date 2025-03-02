import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_dim_store_branch(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform store branch data from staging to the warehouse schema (dim_store_branch).
    """
    try:
        data = data.rename(columns={
            'store_id': 'nk_store_id',
            'store_name': 'store_name',
            'created_at': 'created_at'
        })

        # Drop duplicates based on nk_store_id
        data = data.drop_duplicates(subset=['nk_store_id'])

        # Log success
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "dim_store_branch",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return data
    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "dim_store_branch",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
        handle_error(data, bucket_name='error-dellstore', table_name='dim_store_branch', step='warehouse', component='transformation')
    finally:
        etl_log(log_msg)
