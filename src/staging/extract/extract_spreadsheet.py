import pandas as pd
from src.utils.helper import etl_log
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def auth_gspread():
    """
    Authenticates with Google Sheets API.
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    #Define your credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('CRED_PATH'), scope)
    return gspread.authorize(credentials)

def init_key_file(key_file:str):
    #define credentials to open the file
    gc = auth_gspread()
    
    #open spreadsheet file by key
    sheet_result = gc.open_by_key(key_file)
    
    return sheet_result

def extract_sheet(key_file: str, worksheet_name: str) -> pd.DataFrame:
    """
    Extracts data from a Google Sheet.
    """
    try:
        # init sheet
        sheet_result = init_key_file(key_file)
        
        worksheet_result = sheet_result.worksheet(worksheet_name)
        
        df_result = pd.DataFrame(worksheet_result.get_all_values())
        
        # set first rows as columns
        df_result.columns = df_result.iloc[0]
        
        # get all the rest of the values
        df_result = df_result[1:,:-1].copy()

        # Log success
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "success",
            # "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return df_result
    
    except Exception as e:
        # Log failure
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "failed",
            # "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }
        print(e)
    finally:
        etl_log(log_msg)