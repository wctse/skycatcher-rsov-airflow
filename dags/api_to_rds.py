"""
DAG: api_to_rds

This DAG demonstrates:
1) Fetching data from an HTTP endpoint (JSON) -> storing raw JSON
2) Transforming data by calling a config-specific function -> storing CSV
3) Slicing rows based on the latest date in RDS
4) Uploading the new rows into RDS

We remove any references to "type" and directly use the "transform_function" specified in CONFIGS.
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine, text

# ------------------------------------------------------------------------------------
# Global settings
# ------------------------------------------------------------------------------------

CONFIGS = [
    {
        "rds_table_name": "near_staking_data_flipside",
        "api_url": "https://flipsidecrypto.xyz/api/v1/queries/23d9a8c4-0ab8-417b-846f-8a9b43d7780c/data/latest",
        "date_column": "day",
        "transform_function": "convert_flipside_json_to_df",
    },
]

DO_NOT_UPLOAD = False
DEV_MODE = False

# Create a connection to RDS
rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)

BASE_DATA_DIR = "/tmp/api_to_rds"
os.makedirs(BASE_DATA_DIR, exist_ok=True)

# ------------------------------------------------------------------------------------
# Utility / transform functions
# ------------------------------------------------------------------------------------

def ensure_dir(directory):
    """Ensures a directory exists, creating it if necessary."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")

def convert_flipside_json_to_df(json_data):
    """
    Example transform function that converts Flipside's JSON to a DataFrame
    with columns [day, stake_amount, unstake_amount, total_supply, price].
    """
    df = pd.DataFrame(json_data)
    df.rename(
        columns={
            "DATE": "day",
            "STAKE_AMOUNT": "stake_amount",
            "UNSTAKE_AMOUNT": "unstake_amount",
            "TOTAL_SUPPLY": "total_supply",
            "PRICE": "price",
        },
        inplace=True
    )
    # Convert the 'day' column to datetime
    df["day"] = pd.to_datetime(df["day"])
    return df

# For convenience, map the function name string to the actual Python function:
TRANSFORM_FUNCTIONS = {
    "convert_flipside_json_to_df": convert_flipside_json_to_df
}

# ------------------------------------------------------------------------------------
# Step 1: Fetch the latest date from an RDS table
# ------------------------------------------------------------------------------------

def fetch_rds_table_dates(rds_table_name, date_column, **kwargs):
    """
    Gets the MAX(date_column) from the specified RDS table.
    Writes to /tmp/api_to_rds/<table_name>_latest_rds_date.csv
    """
    try:
        ensure_dir(BASE_DATA_DIR)
        out_csv = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_latest_rds_date.csv")

        query = text(f"SELECT COALESCE(MAX({date_column})::text, '1900-01-01') as max_date FROM {rds_table_name}")
        df = pd.read_sql(query, rds_engine)
        df.to_csv(out_csv, index=False)

        print(f"Fetched latest RDS date for table {rds_table_name}: {df['max_date'][0]}")
        print(f"Saved to {out_csv}")

    except Exception as e:
        raise AirflowException(f"Error in fetch_rds_table_dates({rds_table_name}): {e}")

# ------------------------------------------------------------------------------------
# Step 2: Fetch data from the HTTP endpoint (store raw JSON)
# ------------------------------------------------------------------------------------

def fetch_api_data(config_item, **kwargs):
    """
    1) Fetches data from config_item['api_url'] as JSON
    2) Saves the raw JSON to /tmp/api_to_rds/<table>_raw.json
    """
    rds_table_name = config_item["rds_table_name"]
    out_json = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_raw.json")

    try:
        ensure_dir(BASE_DATA_DIR)

        api_url = config_item["api_url"]
        resp = requests.get(api_url)
        if resp.status_code != 200:
            raise AirflowException(f"Failed to fetch data from {api_url}. Code: {resp.status_code}")

        data = resp.json()
        with open(out_json, "w") as f:
            json.dump(data, f, indent=2)

        if DEV_MODE:
            print(f"DEV_MODE=True -> Raw JSON for table {rds_table_name} from {api_url}:")
            print(data)

        print(f"Fetched JSON from {api_url}, wrote to {out_json}")

    except Exception as e:
        raise AirflowException(f"Error in fetch_api_data for table {rds_table_name}: {e}")

# ------------------------------------------------------------------------------------
# Step 3: Transform the raw JSON into a CSV using the config-based function
# ------------------------------------------------------------------------------------

def transform_data(config_item, **kwargs):
    """
    Reads the raw JSON from /tmp/api_to_rds/<table>_raw.json,
    applies the user-specified transform function, and writes
    the transformed DataFrame to <table>_transformed.csv.
    """
    rds_table_name = config_item["rds_table_name"]
    raw_json_path = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_raw.json")
    out_csv = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_transformed.csv")

    try:
        if not os.path.exists(raw_json_path):
            raise ValueError(f"Raw JSON file not found at {raw_json_path}")

        # Load the raw JSON
        with open(raw_json_path, "r") as f:
            raw_data = json.load(f)

        # Lookup and call the transform function specified in config
        tfunc_name = config_item["transform_function"]
        if tfunc_name not in TRANSFORM_FUNCTIONS:
            raise ValueError(f"Transform function '{tfunc_name}' is not defined.")
        transform_func = TRANSFORM_FUNCTIONS[tfunc_name]

        # Transform the JSON into a DataFrame
        df = transform_func(raw_data)

        # Ensure the date_column is properly set as datetime during transformation
        date_column = config_item["date_column"]
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])

        df.to_csv(out_csv, index=False)

        if DEV_MODE:
            print(f"DEV_MODE=True -> Transformed data for {rds_table_name}:")
            print(df.to_string())

        print(f"Transformed JSON -> CSV. Saved {len(df)} rows to {out_csv}")

    except Exception as e:
        raise AirflowException(f"Error in transform_data({rds_table_name}): {e}")


# ------------------------------------------------------------------------------------
# Step 4: Slice the data (filter rows by latest RDS date)
# ------------------------------------------------------------------------------------

def slice_data(rds_table_name, date_column, **kwargs):
    """
    Reads <table>_latest_rds_date.csv and <table>_transformed.csv,
    filters out rows with date <= max_date,
    writes the remainder to <table>_to_upload.csv.
    """
    rds_date_file = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_latest_rds_date.csv")
    transformed_csv = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_transformed.csv")
    out_file = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_to_upload.csv")

    try:
        if not os.path.exists(rds_date_file):
            raise ValueError(f"Cannot slice data because RDS date file not found: {rds_date_file}")
        if not os.path.exists(transformed_csv):
            raise ValueError(f"Cannot slice data because transformed CSV not found: {transformed_csv}")

        rds_df = pd.read_csv(rds_date_file)
        if rds_df.empty:
            raise ValueError(f"No date found in {rds_date_file}")
        else:
            latest_str = rds_df['max_date'][0]

        latest_date = pd.to_datetime(latest_str)
        transformed_df = pd.read_csv(transformed_csv)

        # Filter rows where the date is greater than the latest_date
        filtered_df = transformed_df[pd.to_datetime(transformed_df[date_column]) > latest_date].copy()
        filtered_df.to_csv(out_file, index=False)

        print(f"Sliced data: {len(filtered_df)} rows remain (date > {latest_date}).")
        print(f"Saved sliced data to {out_file}")

    except Exception as e:
        raise AirflowException(f"Error in slice_data({rds_table_name}): {e}")


# ------------------------------------------------------------------------------------
# Step 5: Upload to RDS
# ------------------------------------------------------------------------------------

def upload_to_rds(csv_file_name, table_name, **kwargs):
    """
    Uploads the specified CSV (csv_file_name) into the RDS table (table_name).
    If DO_NOT_UPLOAD is True, prints the data instead.
    """
    try:
        csv_path = os.path.join(BASE_DATA_DIR, csv_file_name)
        if not os.path.exists(csv_path):
            print(f"{csv_path} not found. Skipping upload to {table_name}.")
            return

        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"No rows to upload in {csv_file_name}.")
            return

        if DO_NOT_UPLOAD:
            print(f"DO_NOT_UPLOAD=True -> Printing data for table {table_name}:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
        else:
            df.to_sql(
                table_name,
                rds_engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"Uploaded {len(df)} rows into RDS table {table_name}")

    except Exception as e:
        raise AirflowException(f"Error in upload_to_rds({table_name}): {e}")

# ------------------------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    dag_id='api_to_rds',
    default_args=default_args,
    description='Fetch data from an HTTP endpoint (raw JSON), transform, slice, and upload to RDS.',
    schedule_interval=timedelta(days=100000),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
)

for cfg in CONFIGS:
    table = cfg["rds_table_name"]
    base_id = table.lower()

    # 1) Get RDS date
    get_rds_date_task = PythonOperator(
        task_id=f"get_rds_date_{base_id}",
        python_callable=fetch_rds_table_dates,
        op_kwargs={"rds_table_name": table, "date_column": cfg["date_column"]},
        dag=dag
    )

    # 2) Fetch new data from the HTTP API (raw JSON)
    fetch_api_data_task = PythonOperator(
        task_id=f"fetch_api_data_{base_id}",
        python_callable=fetch_api_data,
        op_kwargs={"config_item": cfg},
        dag=dag
    )

    # 3) Transform data
    transform_data_task = PythonOperator(
        task_id=f"transform_data_{base_id}",
        python_callable=transform_data,
        op_kwargs={"config_item": cfg},
        dag=dag
    )

    # 4) Slice data
    slice_data_task = PythonOperator(
        task_id=f"slice_data_{base_id}",
        python_callable=slice_data,
        op_kwargs={"rds_table_name": table, "date_column": cfg["date_column"]},
        dag=dag
    )

    # 5) Upload to RDS
    upload_task = PythonOperator(
        task_id=f"upload_to_rds_{base_id}",
        python_callable=upload_to_rds,
        op_kwargs={
            "csv_file_name": f"{table}_to_upload.csv",
            "table_name": table
        },
        dag=dag
    )

    # Define task dependencies
    get_rds_date_task >> fetch_api_data_task >> transform_data_task >> slice_data_task >> upload_task
