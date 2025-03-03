import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_fct_inventory(data: pd.DataFrame, df_products_tf: pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms inventory data from staging into the fct_inventory table in the warehouse.
    """
    try:
        # Rename columns
        data = data.rename(columns={
            'tracking_id': 'nk_tracking_id',
            'product_id': 'nk_product_id',
            'quantity_change': 'quantity_change',
            'change_date': 'change_date',
            'reason': 'reason',
            'created_at': 'created_at'
        })

        # Merge with products to get product keys
        data['nk_product_id'] = data["nk_product_id"].apply(lambda x: df_products_tf.loc[df_products_tf['nk_product_id'] == x, 'sk_product_id'].values[0])

        # Convert change_date to integer (timestamp in DWH)
        data['change_date'] = pd.to_datetime(data['change_date']).astype(int) / 10**9  # Convert to Unix timestamp (int)

        # Drop duplicates based on nk_tracking_id
        data = data.drop_duplicates(subset=['nk_tracking_id'])

        # Log success
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "fct_inventory",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return data
    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "fct_inventory",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
        handle_error(data, bucket_name='error-dellstore', table_name='fct_inventory', step='warehouse', component='transformation')
    finally:
        etl_log(log_msg)
