import pandas as pd
from datetime import datetime
from src.utils.helper import get_db_connection, read_etl_log, etl_log

def extract_staging(table_name: str, schema_name: str):
    """
    This function is used to extract data from the staging database. 
    """    
    try:
        # create connection to database staging
        conn = get_db_connection('staging')

        # Get date from previous process
        filter_log = {"step": "warehouse",
                    "table_name": table_name,
                    "status": "success",
                    "component": "load"}
        etl_date = read_etl_log(filter_log)

        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if etl_date.empty or etl_date['max'][0] is None:
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date['max'][0]
            # etl_date = etl_date.strftime("%Y-%m-%d")

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        query = f"SELECT * FROM {schema_name}.{table_name} WHERE created_at > %s::timestamp"

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(etl_date,))
        log_msg = {
                "step" : "warehouse",
                "component":"extraction",
                "status": "success",
                # "source": "database",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
        return df
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component":"extraction",
            "status": "failed",
            # "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
        print(e)
    finally:
        # Save the log message
        etl_log(log_msg)