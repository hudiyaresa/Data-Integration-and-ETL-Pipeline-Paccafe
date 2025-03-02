from src.staging.extract.extract_db import extract_database
from src.staging.extract.extract_spreadsheet import extract_sheet
from src.staging.load.load_staging import load_staging
# from src.staging.load.load_minio import handle_error
from datetime import datetime
import os

def staging_pipeline():
    # Extract data from database
    df_customers = extract_database('customers')
    df_employees = extract_database('employees')
    df_products = extract_database('products')
    df_orders = extract_database('orders')
    df_order_details = extract_database('order_details')
    df_inventory = extract_database('inventory_tracking')
    
    # Extract data from spreadsheet
    df_store_branch = extract_sheet(os.getenv('KEY_SPREADSHEET'), 'store_branch')
    
    # Load data into staging (except last column, created_at)
    load_staging(data=df_customers, schema='public', table_name='customers', idx_name='customer_id')
    load_staging(data=df_employees, schema='public', table_name='employees', idx_name='employee_id')
    load_staging(data=df_products, schema='public', table_name='products', idx_name='product_id')
    load_staging(data=df_orders, schema='public', table_name='orders', idx_name='order_id')
    load_staging(data=df_order_details, schema='public', table_name='order_details', idx_name='order_detail_id')
    load_staging(data=df_inventory, schema='public', table_name='inventory_tracking', idx_name='tracking_id')
    load_staging(data=df_store_branch, schema='public', table_name='store_branch', idx_name='store_id')