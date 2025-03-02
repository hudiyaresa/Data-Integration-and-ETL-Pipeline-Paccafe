import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_fct_order(data: pd.DataFrame, df_customers: pd.DataFrame, df_employees: pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms order data from staging into the fct_order table in the warehouse.
    """
    try:
        # Rename columns to match the warehouse schema
        data = data.rename(columns={
            'order_id': 'nk_order_id',
            'employee_id': 'sk_employee_id',
            'customer_id': 'sk_customer_id',
            'order_date': 'order_date',
            'total_amount': 'total_amount',
            'payment_method': 'payment_method',
            'order_status': 'order_status',
            'created_at': 'created_at'
        })

        # Merge with customers and employees to get the customer and employee keys
        data = data.merge(df_customers[['customer_id', 'nk_customer_id']], on='customer_id', how='left')
        data = data.merge(df_employees[['employee_id', 'nk_employee_id']], on='employee_id', how='left')

        # Convert order_date to integer (timestamp in DWH)
        data['order_date'] = pd.to_datetime(data['order_date']).astype(int) / 10**9  # Convert to Unix timestamp (int)

        # Drop duplicates based on nk_order_id
        data = data.drop_duplicates(subset=['nk_order_id'])

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
        handle_error(data, bucket_name='error-dellstore', table_name='fct_order', step='warehouse', component='transformation')
    finally:
        etl_log(log_msg)
