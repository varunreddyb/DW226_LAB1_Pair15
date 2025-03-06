from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import yfinance as yf
import pandas as pd
import snowflake.connector
import os
import logging

# Fetch Snowflake credentials from Airflow Variables
SNOWFLAKE_USER = Variable.get("snowflake_user")
SNOWFLAKE_PASSWORD = Variable.get("snowflake_password")
SNOWFLAKE_ACCOUNT = Variable.get("snowflake_account")
SNOWFLAKE_WAREHOUSE = Variable.get("snowflake_warehouse")
SNOWFLAKE_DATABASE = Variable.get("snowflake_database")
SNOWFLAKE_SCHEMA = Variable.get("snowflake_schema")

# Local file path for temporary CSV storage
LOCAL_FILE_PATH = "stock_data.csv"

# Define default_args for Airflow
default_args = {
    'owner': 'airflow',
    'start_date': datetime(),
    'retries': 1,
}

# Define the Airflow DAG
dag = DAG(
    'stock_price_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Function to create Snowflake table
def create_snowflake_table():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE OR REPLACE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STOCK_DATA (
                SYMBOL STRING,
                DATE DATE,
                OPEN FLOAT,
                CLOSE FLOAT,
                LOW FLOAT,
                HIGH FLOAT,
                VOLUME INTEGER
            )
        """)

        logging.info("✅ Table STOCK_DATA created successfully in Snowflake!")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Error in create_snowflake_table: {str(e)}")

# Function to fetch stock data using yfinance.download()
def fetch_stock_data():
    stocks = ["GOOGL", "AAPL"]

    try:
        # Download data for multiple stocks using yfinance
        df_list = []
        for stock in stocks:
            df = yf.download(stock, period="180d", interval="1d")
            df.reset_index(inplace=True)
            df["Symbol"] = stock  # Add stock symbol column
            df_list.append(df)

        # Combine data for all stocks into a single DataFrame
        df_final = pd.concat(df_list, ignore_index=True)

        # Rename columns to match Snowflake table schema
        df_final.rename(columns={
            "Date": "DATE",
            "Open": "OPEN",
            "Close": "CLOSE",
            "Low": "LOW",
            "High": "HIGH",
            "Volume": "VOLUME"
        }, inplace=True)

        # Ensure DATE column is in correct format
        df_final["DATE"] = pd.to_datetime(df_final["DATE"]).dt.strftime('%Y-%m-%d')

        # Save to CSV for staging
        df_final.to_csv(LOCAL_FILE_PATH, index=False, header=True)

        logging.info(f"✅ Stock data saved to {LOCAL_FILE_PATH}")
    except Exception as e:
        logging.error(f"❌ Error in fetch_stock_data: {str(e)}")

# Function to load data into Snowflake using COPY INTO
def load_into_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()

        # Create a temporary stage in Snowflake
        stage_name = "TEMP_STAGE"
        cursor.execute(f"CREATE TEMPORARY STAGE {stage_name}")

        # Upload the CSV file to the temporary stage
        cursor.execute(f"PUT file://{LOCAL_FILE_PATH} @{stage_name}")

        # Use COPY INTO to load data into Snowflake efficiently
        cursor.execute(f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STOCK_DATA
            FROM @{stage_name}/{os.path.basename(LOCAL_FILE_PATH)}
            FILE_FORMAT = (
                TYPE = 'CSV',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
        """)

        logging.info("✅ Data successfully loaded into Snowflake using COPY INTO!")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Error in load_into_snowflake: {str(e)}")

# Function to run the full ETL pipeline
def run_pipeline():
    try:
        create_snowflake_table()
        fetch_stock_data()
        load_into_snowflake()
        logging.info("✅ ETL pipeline completed successfully!")
    except Exception as e:
        logging.error(f"❌ Error in run_pipeline: {str(e)}")

# Create Airflow tasks
run_stock_pipeline = PythonOperator(
    task_id='run_stock_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_stock_pipeline  # Start the pipeline
