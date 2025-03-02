from src.warehouse.extract.extract import extract_staging
from src.warehouse.transform.transform_dim_customers import transform_dim_customers
from src.warehouse.transform.transform_dim_employees import transform_dim_employees
from src.warehouse.transform.transform_dim_products import transform_dim_products
from src.warehouse.transform.transform_dim_store_branch import transform_dim_store_branch
from src.warehouse.transform.transform_fct_order import transform_fct_order
from src.warehouse.transform.transform_fct_inventory import transform_fct_inventory
from src.warehouse.load.load import load_warehouse

def warehouse_pipeline():
    # Extract data from staging
    df_customers = extract_staging('customers', 'public')
    df_employees = extract_staging('employees', 'public')
    df_store_branch = extract_staging('store_branch', 'public')
    df_products = extract_staging('products', 'public')
    df_orders = extract_staging('orders', 'public')
    df_order_details = extract_staging('order_details', 'public')
    df_inventory = extract_staging('inventory_tracking', 'public')
    
    # Transform data
    df_customers_tf = transform_dim_customers(df_customers)
    df_employees_tf = transform_dim_employees(df_employees)
    df_store_branch_tf = transform_dim_store_branch(df_store_branch)
    df_products_tf = transform_dim_products(df_products, df_store_branch)
    df_orders_tf = transform_fct_order(df_orders, df_customers, df_employees, df_order_details)  # merging with other data
    df_inventory_tf = transform_fct_inventory(df_inventory, df_products)  # merging with other data
    
    # Load data into warehouse
    load_warehouse(df_customers_tf, 'public', 'dim_customers', 'nk_customer_id', 'staging')
    load_warehouse(df_employees_tf, 'public', 'dim_employees', 'nk_employee_id', 'staging')
    load_warehouse(df_store_branch_tf, 'public', 'dim_store_branch', 'nk_store_id', 'staging')
    load_warehouse(df_products_tf, 'public', 'dim_products', 'nk_product_id', 'staging')
    load_warehouse(df_orders_tf, 'public', 'fct_order', 'nk_order_id', 'staging')
    load_warehouse(df_inventory_tf, 'public', 'fct_inventory', 'nk_tracking_id', 'staging')
