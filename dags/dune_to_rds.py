"""
DAG: dune_to_rds

This DAG reads from a small CONFIGS list, which indicates whether to fetch data from a 
Dune materialized view or from a specific query. After fetching existing 'latest date' 
from RDS, it downloads data from Dune, slices rows that are already in RDS, and then 
uploads only new data. The upload_to_rds() function supports a dry-run approach if 
DO_NOT_UPLOAD=True, and fetch_dune_table_dates() is adapted to accept a query_id argument 
directly.

In the case of a "materialized_view" configuration, the DAG calls the 
/api/v1/materialized-views/{name} endpoint, parses the s3_url, and saves the CSV. 
If the config type is "query", it runs the query using dune_client, then saves the 
results as a CSV.
"""

import os
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

from dune_client.client import DuneClient
from dune_client.query import QueryBase

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# ------------------------------------------------------------------------------------
# Global settings
# ------------------------------------------------------------------------------------

CONFIGS = [
    # {
    #     "rds_table_name": "eth_staking_value_dune_constructed",
    #     "type": "query",
    #     "query_id": 4480493,
    #     "date_column": "date",
    # },
    # {
    #     "rds_table_name": "sol_staking_value_dune_21co",
    #     "type": "query",
    #     "query_id": 4460227,
    #     "date_column": "day",
    # },
    {
        "rds_table_name": "prices_usd_daily",
        "type": "query",
        "query_id": 4504144,
        "date_column": "day",
    },
    # {
    #     "rds_table_name": "sonic_staking_value_dune_constructed",
    #     "type": "query",
    #     "query_id": 4794474,
    #     "date_column": "date",
    # }
    {
        "rds_table_name": "aero_buybacks_newlocks",
        "type": "query",
        "query_id": 4632140,
        "date_column": "t",
    },
    {
        "rds_table_name": "aero_emission_vs_locked_epoch_breakdown",
        "type": "query",
        "query_id": 4713644,
        "date_column": "epoch",
    },
    {
        "rds_table_name": "aero_price_total_supply",
        "type": "query",
        "query_id": 5326587,
        "date_column": "day",
    }
]

DO_NOT_UPLOAD = False
DEV_MODE = False

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)

BASE_DATA_DIR = "/tmp/dune_to_rds"
os.makedirs(BASE_DATA_DIR, exist_ok=True)

# ------------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------------

def load_api_key(key_name):
    """
    Loads the specified API key from Airflow Variables.
    """
    try:
        return Variable.get(key_name)
    except KeyError:
        print(f"API key '{key_name}' not found in Variables.")
        return None

def ensure_dir(directory):
    """
    Ensures a directory exists, creating it if necessary.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")

def fetch_dune_table_dates(query_id, **kwargs):
    """
    Fetches date information or any result columns from a specific Dune query. 
    Saves the results to a CSV with the query_id in its filename.
    This can be reused or adapted as needed in multiple DAGs.
    """
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found.")

        dune = DuneClient(api_key)
        query = QueryBase(
            name="Fetch table dates",
            query_id=query_id
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)

        ensure_dir(BASE_DATA_DIR)
        output_file = os.path.join(BASE_DATA_DIR, f"latest_dune_dates_{query_id}.csv")
        df.to_csv(output_file, index=False)
        print(f"Fetched data from Dune query_id={query_id}, rows={len(df)}")
        print(f"Saved to {output_file}")
    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_table_dates: {e}")

# ------------------------------------------------------------------------------------
# Step 1: Fetch the latest date from an RDS table
# ------------------------------------------------------------------------------------

def fetch_rds_table_dates(rds_table_name, date_column, **kwargs):
    """
    Gets the MAX(date) from the specified RDS table. 
    Writes to /tmp/dune_to_rds/<table_name>_latest_rds_date.csv
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
# Step 2: Fetch data from Dune (either matview or query) and write to CSV
# ------------------------------------------------------------------------------------

def fetch_dune_data(config_item, **kwargs):
    """
    Fetches data from either a materialized view or a query, 
    based on 'type' in config_item. Writes to /tmp/dune_to_rds/<table>_raw.csv
    """
    rds_table_name = config_item["rds_table_name"]
    out_csv = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_raw.csv")

    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found.")

        ensure_dir(BASE_DATA_DIR)

        # Query approach via dune_client
        if config_item["type"] == "query":
            query_id = config_item["query_id"]
            dune = DuneClient(api_key)
            query = QueryBase(name=f"Fetch {rds_table_name}", query_id=query_id)
            results = dune.run_query(query)

            df = pd.DataFrame(results.result.rows)
            df.to_csv(out_csv, index=False)

            if DEV_MODE:
                print(f"DEV_MODE=True -> Printing data for table {rds_table_name} from Dune query_id={query_id}:")
                print(df.to_string())
                print(f"Total rows: {len(df)}")

            print(f"Fetched {len(df)} rows from Dune query_id={query_id} and saved to {out_csv}")

        # # Materialized View approach
        # # Not working at the moment
        # elif config_item["type"] == "materialized_view":
        #     matview_name = config_item["dune_materialized_view"]
        #     url = f"https://api.dune.com/api/v1/materialized-views/{matview_name}"
        #     resp = requests.get(url, headers={"X-Dune-Api-Key": api_key})
            
        #     if resp.status_code != 200:
        #         raise AirflowException(f"Failed to fetch matview {matview_name}. Code: {resp.status_code}")
            
        #     resp_json = resp.json()
        #     s3_url = resp_json.get("s3_url")
        #     if not s3_url:
        #         raise AirflowException(f"No s3_url in matview response for {matview_name}.")

        #     s3_resp = requests.get(s3_url)

        #     if s3_resp.status_code != 200:
        #         raise AirflowException(f"Could not download CSV from {s3_url}. Code: {s3_resp.status_code}")

        #     with open(out_csv, "wb") as f:
        #         f.write(s3_resp.content)

        #     if DEV_MODE:
        #         print(f"DEV_MODE=True -> Printing data for table {rds_table_name} from Dune matview {matview_name}:")
        #         print(df.to_string())
        #         print(f"Total rows: {len(df)}")

        #     print(f"Fetched materialized view data for {matview_name} and saved to {out_csv}")
        
        else:
            raise ValueError(f"Unknown config type: {config_item['type']}")

    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_data for table {rds_table_name}: {e}")

# ------------------------------------------------------------------------------------
# Step 3: Slice the data so we only keep rows after the latest RDS date
# ------------------------------------------------------------------------------------

def slice_data(rds_table_name, date_column, **kwargs):
    """
    Reads <table>_latest_rds_date.csv and <table>_raw.csv,
    filters out rows with date <= max_date, 
    and writes the remainder to <table>_to_upload.csv
    """
    rds_date_file = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_latest_rds_date.csv")
    raw_file = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_raw.csv")
    out_file = os.path.join(BASE_DATA_DIR, f"{rds_table_name}_to_upload.csv")

    try:
        if not os.path.exists(rds_date_file):
            raise ValueError(f"Cannot slice data because RDS date file not found: {rds_date_file}")
        if not os.path.exists(raw_file):
            raise ValueError(f"Cannot slice data because raw file not found: {raw_file}")

        rds_df = pd.read_csv(rds_date_file)
        if rds_df.empty:
            raise ValueError(f"No date found in {rds_date_file}")
        else:
            latest_str = rds_df['max_date'][0]

        latest_date = pd.to_datetime(latest_str)
        raw_df = pd.read_csv(raw_file)

        if date_column not in raw_df.columns:
            raise ValueError(f"No '{date_column}' column found in raw data. Skipping slicing logic. Columns: {raw_df.columns}")

        raw_df[date_column] = pd.to_datetime(raw_df[date_column])
        raw_df[date_column] = pd.to_datetime(raw_df[date_column], utc=True)
        latest_date = pd.to_datetime(latest_date, utc=True)
        filtered_df = raw_df[raw_df[date_column] > latest_date].copy()
        filtered_df.to_csv(out_file, index=False)

        print(f"Sliced data: {len(filtered_df)} rows remain (date > {latest_date}).")
        print(f"Saved sliced data to {out_file}")

    except Exception as e:
        raise AirflowException(f"Error in slice_data({rds_table_name}): {e}")

# ------------------------------------------------------------------------------------
# Step 4: upload_to_rds() - dynamically sets CSV file name and table name
# ------------------------------------------------------------------------------------

def upload_to_rds(csv_file_name, table_name, **kwargs):
    """
    Uploads the specified CSV (csv_file_name) into the RDS table (table_name).
    If DO_NOT_UPLOAD is True, it prints the data instead.
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
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='dune_to_rds',
    default_args=default_args,
    description=get_dag_config('dune_to_rds')['description'],
    schedule_interval=get_schedule_interval('dune_to_rds'),
    start_date=get_start_date('dune_to_rds'),
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

    # 2) Fetch new data from Dune
    fetch_dune_data_task = PythonOperator(
        task_id=f"fetch_dune_data_{base_id}",
        python_callable=fetch_dune_data,
        op_kwargs={"config_item": cfg},
        dag=dag
    )

    # 3) Slice data
    slice_data_task = PythonOperator(
        task_id=f"slice_data_{base_id}",
        python_callable=slice_data,
        op_kwargs={"rds_table_name": table, "date_column": cfg["date_column"]},
        dag=dag
    )

    # 4) Upload to RDS
    upload_task = PythonOperator(
        task_id=f"upload_to_rds_{base_id}",
        python_callable=upload_to_rds,
        op_kwargs={
            "csv_file_name": f"{table}_to_upload.csv",
            "table_name": table
        },
        dag=dag
    )

    get_rds_date_task >> fetch_dune_data_task >> slice_data_task >> upload_task