import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_fct_order(data: pd.DataFrame, df_customers_tf: pd.DataFrame, df_employees_tf: pd.DataFrame, df_order_details: pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms order data from staging into the fct_order table in the warehouse.
    """
    try:
        # Rename columns to match the warehouse schema
        data = data.rename(columns={
            'order_id': 'nk_order_id',
            'employee_id': 'nk_employee_id',
            'customer_id': 'nk_customer_id',
            'order_date': 'order_date',
            'total_amount': 'total_amount',
            'payment_method': 'payment_method',
            'order_status': 'order_status',
            'created_at': 'created_at'
        })

        # Extract data from df_employees_tf
        data['sk_employee_id'] = data["nk_employee_id"].apply(
            lambda x: df_employees_tf.loc[df_employees_tf['nk_employee_id'] == x, 'sk_employee_id'].values[0]
        )
        
        # Extract data from df_customers_tf
        data['sk_customer_id'] = data["nk_customer_id"].apply(
            lambda x: df_customers_tf.loc[df_customers_tf['nk_customer_id'] == x, 'sk_customer_id'].values[0]
            if pd.notnull(x) and any(df_customers_tf["nk_customer_id"] == x) else None
        )

        # Merge with customers and employees to get the customer and employee keys
        # data = data.merge(df_customers_tf[['customer_id', 'nk_customer_id']], on='customer_id', how='left')
        # data = data.merge(df_employees_tf[['employee_id', 'nk_employee_id']], on='employee_id', how='left')
        # data = data.merge(df_order_details[['order_id', 'nk_order_id']], on='order_id', how='left')

        # Convert order_date to integer (timestamp in DWH)
        data['order_date'] = data['order_date'].dt.strftime('%Y%m%d').astype(int)

        # Drop duplicates based on nk_order_id
        data = data.drop_duplicates(subset=['nk_order_id'])

        # Drop unnecessary columns
        data = data.drop(columns=["nk_employee_id", "nk_customer_id"])    

        # Log success
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "fct_order",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return data
    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "fct_order",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
        handle_error(data, bucket_name='error-paccafe', table_name='fct_order', step='warehouse', component='transformation')
    finally:
        etl_log(log_msg)
