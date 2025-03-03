import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime
import re

def transform_dim_products(data: pd.DataFrame, df_store_branch_tf: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to transform product data from staging to the data warehouse.
    Handles negative values in `unit_price` and `cost_price` by converting them to absolute values.
    """
    try:
        # Rename columns to match the warehouse schema
        data = data.rename(columns={
            'product_id': 'nk_product_id',
            'product_name': 'product_name',
            'category': 'category',
            'unit_price': 'unit_price',
            'cost_price': 'cost_price',
            'in_stock': 'in_stock',
            'store_branch': 'store_name',
            'created_at': 'created_at'
        })

        # Drop duplicate nk_product_id if any
        data = data.drop_duplicates(subset="nk_product_id")        

        # Extract data from dim_store_branch
        data['sk_store_branch'] = data["store_name"].apply(lambda x: df_store_branch_tf.loc[df_store_branch_tf['store_name'] == x, 'sk_store_id'].values[0])

        # Convert price columns to numeric
        data['unit_price'] = pd.to_numeric(data['unit_price'].apply(lambda x: re.sub(r'\D', '', str(x))), errors='coerce')
        data['cost_price'] = pd.to_numeric(data['cost_price'].apply(lambda x: re.sub(r'\D', '', str(x))), errors='coerce')        

        # Ensure no negative values in the price fields
        data['unit_price'] = data['unit_price'].apply(lambda x: re.sub(r'\s*-', '', str(x)))
        data['cost_price'] = data['cost_price'].apply(lambda x: re.sub(r'\s*-', '', str(x)))

        # Drop unnecessary columns and handle nulls
        data = data.drop(columns=['nk_store_id', 'store_name'])
        data = data.dropna(subset=['nk_product_id'])

        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "dim_products",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return data

    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "dim_products",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(f"Error in transformation: {e}")
        handle_error(data, bucket_name='error-paccafe', table_name='dim_products', step='warehouse', component='transformation')

    finally:
        etl_log(log_msg)
