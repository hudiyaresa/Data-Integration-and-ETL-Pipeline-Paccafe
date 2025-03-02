import pandas as pd
from src.utils.helper import etl_log, handle_error
from datetime import datetime

def transform_dim_employees(data: pd.DataFrame) -> pd.DataFrame:
    """
    This function transforms employee data, filtering out rows with inconsistent roles.
    """
    try:
        # Filter out inconsistent roles
        data = data[~data['role'].isin(['today', 'third', 'me'])]

        # Rename columns to match the warehouse schema
        data = data.rename(columns={
            'employee_id': 'nk_employee_id',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'hire_date': 'hire_date',
            'role': 'role',
            'email': 'email',
            'created_at': 'created_at'
        })

        # Drop rows where the role or other important fields are missing
        data = data.dropna(subset=['role', 'first_name', 'last_name'])

        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "success",
            # "source": "staging",
            "table_name": "dim_employees",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return data

    except Exception as e:
        log_msg = {
            "step": "warehouse",
            "component": "transformation",
            "status": "failed",
            # "source": "staging",
            "table_name": "dim_employees",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(f"Error in transformation: {e}")
        handle_error(data, bucket_name='error-dellstore', table_name='dim_employees', step='warehouse', component='transformation')

    finally:
        etl_log(log_msg)
