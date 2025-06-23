from prefect import flow, task, get_run_logger
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

@task
def load_and_clean_data():
    logger = get_run_logger()
    logger.info("🔄 Reading and cleaning CSV data...")
    
    df = pd.read_csv("data/raw_data.csv")
    df.dropna(subset=['CustomerID'], inplace=True)
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
    
    logger.info(f"✅ Cleaned {len(df)} rows of data.")
    return df

@task
def load_to_postgres(df):
    logger = get_run_logger()
    logger.info("🚀 Loading data to PostgreSQL...")
    
    load_dotenv()
    engine = create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    df.to_sql('ecommerce_data', engine, if_exists='replace', index=False)
    
    logger.info("🎉 Data load complete.")

@flow(name="ETL Pipeline for E-Commerce")
def etl_pipeline():
    logger = get_run_logger()
    logger.info("🏁 Starting ETL pipeline...")
    
    df = load_and_clean_data()
    load_to_postgres(df)
    
    logger.info("✅ ETL pipeline finished successfully.")

if __name__ == "__main__":
    etl_pipeline()
