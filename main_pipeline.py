from src.staging_pipeline import staging_pipeline
from src.warehouse_pipeline import warehouse_pipeline

if __name__ == "__main__":
    staging_pipeline()
    warehouse_pipeline()