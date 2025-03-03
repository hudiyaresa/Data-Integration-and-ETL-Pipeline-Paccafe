import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_dim_customers(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the customer data from staging to the warehouse schema (dim_customers).
    """
    try:
        # Map staging fields to warehouse fields
        data = data.rename(columns={
            'customer_id': 'nk_customer_id',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'email': 'email',
            'phone': 'phone',
            'loyalty_points': 'loyalty_points',
            'created_at': 'created_at'
        })

        # Drop any duplicate records if any
        data = data.drop_duplicates(subset=['nk_customer_id'])

        # Log success
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "dim_customers",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return data
    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "dim_customers",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
        # Error handling (store failed data)
        handle_error(data, bucket_name='error-paccafe', table_name='dim_customers', step='warehouse', component='transformation')
    finally:
        # Save log
        etl_log(log_msg)
