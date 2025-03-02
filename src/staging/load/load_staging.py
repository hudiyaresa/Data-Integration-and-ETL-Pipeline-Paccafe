import pandas as pd
from src.utils.helper import get_db_connection, etl_log, handle_error
from datetime import datetime
from pangres import upsert

def load_staging(data, schema: str, table_name: str, idx_name: str):
    try:
        conn = get_db_connection('staging')
        data = data.iloc[:, :-1] #Remove crated_at in the last column        
        data = data.set_index(idx_name)

        # Do upsert (Update for existing data and Insert for new data)
        upsert(con=conn, 
               df=data, 
               table_name=table_name, 
               schema=schema, 
               if_row_exists="update")
        
        #create success log message
        log_msg = {
            "step": "staging",
            "component": "load",
            "status": "success",
            # "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        log_msg = {
            "step": "staging",
            "component": "load",
            "status": "failed",
            # "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        
        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error-paccafe', table_name= table_name, step='staging', component='load')
        except Exception as e:
            print(e)

    finally:
        etl_log(log_msg)