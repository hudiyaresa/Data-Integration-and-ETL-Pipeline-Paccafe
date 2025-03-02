import pandas as pd
from src.utils.helper import get_db_connection, etl_log, handle_error
from datetime import datetime
from pangres import upsert

def load_warehouse(data, schema: str, table_name: str, idx_name: str, source: str):
    try:
        # create connection to database
        conn = get_db_connection('warehouse')

        # set data index or primary key
        data = data.set_index(idx_name)

        # Do upsert (Update for existing data and Insert for new data)
        upsert(con=conn, 
               df=data, 
               table_name=table_name, 
               schema=schema, 
               if_row_exists="update")
        
        #create success log message        
        log_msg = {
            "step": "warehouse",
            "component": "load",
            "status": "success",
            # "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # return data
                
    except Exception as e:

        #create fail log message        
        log_msg = {
            "step": "warehouse",
            "component": "load",
            "status": "failed",
            # "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)

        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error-paccafe', table_name= table_name, step='warehouse', component='load')
        except Exception as e:
            print(e)

    finally:
        etl_log(log_msg)
