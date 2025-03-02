import pandas as pd
from sqlalchemy import text 
import sqlalchemy
from src.utils.helper import get_db_connection, etl_log, read_etl_log, read_sql
from datetime import datetime

def extract_database(table_name: str) -> pd.DataFrame:
    """
    Extracts data from the source database incrementally.
    """
    try:
        conn = get_db_connection('source')
        
        # Get the latest etl_date from the log table
        filter_log = {
            "step": "staging",
            "table_name": table_name,
            "status": "success",
            "component": "load"
        }
        etl_date = read_etl_log(filter_log)

        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        # Set etl_date for incremental extraction
        if etl_date.empty or etl_date['max'][0] is None:
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date['max'][0]

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        """
        SELECT * 
        FROM customers 
        WHERE created_at > :etl_date
        """
        query = sqlalchemy.text(read_sql(table_name))
        df = pd.read_sql(sql=query, con=conn, params={"etl_date": etl_date})

        # Log success
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "success",
            # "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return df
    except Exception as e:
        # Log failure
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "failed",
            # "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
    finally:
        etl_log(log_msg)