from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import snowflake.connector
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator  # Correct import for DummyOperator

# Fetch Snowflake credentials from Airflow Variables
SNOWFLAKE_USER = Variable.get("snowflake_user")
SNOWFLAKE_PASSWORD = Variable.get("snowflake_password")
SNOWFLAKE_ACCOUNT = Variable.get("snowflake_account")
SNOWFLAKE_WAREHOUSE = Variable.get("snowflake_warehouse")
SNOWFLAKE_DATABASE = Variable.get("snowflake_database")
SNOWFLAKE_SCHEMA = Variable.get("snowflake_schema")

# Define default_args for Airflow
default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),  # DAG starts from today
    'retries': 1,
}

# Define the Airflow DAG
dag = DAG(
    'forecast_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Runs daily
    catchup=False,
)

def return_snowflake_conn():
    """
    Return a Snowflake connection cursor using Airflow Variables.
    """
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

@task(dag=dag)
def fetch_stock_data_from_snowflake():
    """
    Fetch stock data from Snowflake database.
    """
    conn = return_snowflake_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME
        FROM STOCK.PUBLIC.STOCK_DATA
        WHERE SYMBOL IN ('GOOGL', 'AAPL')
    """)
    
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=column_names)

    logging.info(f"✅ Fetched {len(df)} rows from Snowflake.")
    cursor.close()
    conn.close()

    return df

@task(dag=dag)
def forecast_stock_prices(df):
    """
    Forecast stock prices using ARIMA.
    """
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df.sort_values('DATE')

    forecast_df = pd.DataFrame()

    for symbol in df['SYMBOL'].unique():
        symbol_df = df[df['SYMBOL'] == symbol]  # Filter by symbol
        logging.info(f"Data for {symbol}:\n{symbol_df.head()}")

        # ARIMA model for forecasting
        try:
            model = ARIMA(symbol_df['CLOSE'], order=(5, 1, 0))  # ARIMA model configuration (p, d, q)
            model_fit = model.fit()

            forecast = model_fit.forecast(steps=30)

            last_date = symbol_df['DATE'].max()
            forecast_dates = pd.date_range(start=last_date, periods=30 + 1, freq='D')[1:]

            symbol_forecast_df = pd.DataFrame({
                'DATE': forecast_dates,
                'SYMBOL': symbol,
                'ACTUAL': [None] * 30,
                'FORECAST': forecast
            })

            forecast_df = pd.concat([forecast_df, symbol_forecast_df])

        except Exception as e:
            logging.error(f"❌ Error forecasting {symbol}: {e}")

    logging.info(f"Forecasted data:\n{forecast_df[['DATE', 'SYMBOL', 'ACTUAL', 'FORECAST']].head()}")

    return forecast_df

@task(dag=dag)
def load_forecast_to_snowflake(df):
    """
    Load forecasted results into Snowflake.
    """
    conn = return_snowflake_conn()
    cursor = conn.cursor()

    # Create or replace the forecast table in Snowflake
    cursor.execute("""
        CREATE OR REPLACE TABLE STOCK.PUBLIC.STOCK_FORECAST (
            SYMBOL STRING,
            DATE DATE,
            ACTUAL FLOAT,
            FORECAST FLOAT
        )
    """)

    for _, row in df.iterrows():
        date_str = row['DATE'].strftime('%Y-%m-%d')  # Convert date to string format
        cursor.execute("""
            INSERT INTO STOCK.PUBLIC.STOCK_FORECAST (SYMBOL, DATE, ACTUAL, FORECAST)
            VALUES (%s, %s, %s, %s)
        """, (row['SYMBOL'], date_str, row['ACTUAL'], row['FORECAST']))  # Pass the formatted date

    cursor.close()
    conn.close()

    logging.info("✅ Forecast data successfully loaded into Snowflake!")

@task(dag=dag)
def run_pipeline():
    """
    Run the entire pipeline (fetch data, forecast, load forecast).
    """
    try:
        stock_data = fetch_stock_data_from_snowflake()
        forecasted_data = forecast_stock_prices(stock_data)
        load_forecast_to_snowflake(forecasted_data)
        logging.info("✅ ETL pipeline completed successfully!")
    except Exception as e:
        logging.error(f"❌ Error in ETL pipeline: {str(e)}")

# Trigger another DAG (stock_price_pipeline) at the start
trigger_stock_price_pipeline = TriggerDagRunOperator(
    task_id='trigger_stock_price_pipeline',
    trigger_dag_id='stock_price_pipeline',  # Name of the DAG to be triggered
    dag=dag,
)

# Define the task sequence in the DAG
start_task = DummyOperator(task_id="start", dag=dag)

# Define tasks
etl_task = fetch_stock_data_from_snowflake()
forecasting_task = forecast_stock_prices(etl_task)
load_task = load_forecast_to_snowflake(forecasting_task)

# Set the sequence of task execution
start_task >> trigger_stock_price_pipeline >> etl_task >> forecasting_task >> load_task

