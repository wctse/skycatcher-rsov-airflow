from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests
import os
import time
import logging
import pandas as pd
import csv
import ast

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# Global flags and configuration
DO_NOT_UPLOAD = False  # Set to True to skip actual upload
DATA_DIR = '/tmp/stablecoin_amounts'
TABLE_NAME = 'stablecoin_amounts'

# Ensure DATA_DIR exists
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info(f"Created directory: {DATA_DIR}")

# Create RDS engine using Airflow connection 'rds_connection'
rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)

# Load protocols from CSV file

def load_protocols_from_csv():
    """Load protocols dictionary from /opt/airflow/data/stablecoin protocols.csv
    Returns a dict mapping parentProtocolSlug -> list(childSlugs)
    """
    protocols = {}
    # Match Airflow container data mount convention used elsewhere
    csv_path = "/opt/airflow/data/stablecoin protocols.csv"

    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                parent_slug = row.get("parentProtocolSlug")
                child_slugs_str = row.get("childSlugs", "[]")
                if not parent_slug:
                    continue
                try:
                    child_slugs = ast.literal_eval(child_slugs_str)
                    if isinstance(child_slugs, list):
                        # Clean each slug to string
                        child_slugs = [str(s) for s in child_slugs if s]
                        if child_slugs:
                            protocols[parent_slug] = child_slugs
                        else:
                            logging.warning(f"No child slugs parsed for {parent_slug}")
                    else:
                        logging.warning(f"childSlugs for {parent_slug} is not a list: {child_slugs_str}")
                except (ValueError, SyntaxError) as e:
                    logging.error(f"Failed to parse childSlugs for {parent_slug}: {child_slugs_str}, error: {e}")
        logging.info(f"Loaded {len(protocols)} protocols from {csv_path}")
        return protocols
    except Exception as e:
        logging.error(f"Failed to load protocols from {csv_path}: {e}")
        # Fallback to an empty dict; DAG will no-op gracefully
        return {}

# Load protocols dynamically from CSV
PROTOCOLS = load_protocols_from_csv()

# Define tokens of interest
TOKENS = ['USDT', 'USDC', 'FDUSD', 'PYUSD']

# Utility: Load API key from Airflow Variables

def load_api_key(key_name):
    try:
        return Variable.get(key_name)
    except Exception as e:
        logging.error(f"API key '{key_name}' not found: {e}")
        return None

# Utility: Make an API request with retry logic

def make_api_request(url, method='GET', headers=None, data=None, max_retries=3, retry_delay=60):
    headers = headers or {}
    headers['Content-Type'] = 'application/json'
    last_request_time = getattr(make_api_request, 'last_request_time', 0)

    for attempt in range(max_retries):
        current_time = time.time()
        time_since_last = current_time - last_request_time
        if time_since_last < 1.5:
            time.sleep(1.5 - time_since_last)
        try:
            response = requests.request(method, url, json=data, headers=headers, verify=False)
            make_api_request.last_request_time = time.time()
            logging.info(f"Request URL: {url} - Status: {response.status_code}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                if attempt < max_retries - 1:
                    logging.warning(f"Rate limit hit. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logging.error("Max retries reached for rate limit.")
            else:
                raise AirflowException(f"Error occurred: {response.status_code}, {response.text}")
        except Exception as e:
            logging.error(f"API request failed: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise AirflowException(f"API request failed after {max_retries} attempts: {e}")
    raise AirflowException("Failed to get a valid response from API.")

# Task 1: Get the latest date from the RDS table for incremental processing per (protocol, token).

def get_latest_rds_dates(**kwargs):
    try:
        query = text(f"SELECT protocol, token, MAX(date) as max_date FROM {TABLE_NAME} GROUP BY protocol, token")
        df = pd.read_sql(query, rds_engine)
        latest_dates = {}
        if df.empty:
            latest_dates = {}
        else:
            for _, row in df.iterrows():
                key = (row['protocol'], row['token'])
                if pd.isnull(row['max_date']):
                    latest_dates[key] = datetime(2010, 1, 1).date()
                else:
                    latest_dates[key] = datetime.strptime(str(row['max_date']), "%Y-%m-%d").date()
        logging.info(f"Latest dates by (protocol, token) in {TABLE_NAME}: {latest_dates}")
        return latest_dates
    except Exception as e:
        raise AirflowException(f"Error fetching latest dates from {TABLE_NAME}: {e}")


# Task 2: Fetch API data, process it, and save new records to a CSV file.

def process_stablecoin_amounts(**kwargs):
    # Retrieve the latest dates per (protocol, token) from XCom (as a dictionary)
    ti = kwargs['ti']
    latest_dates = ti.xcom_pull(task_ids='get_latest_rds_dates') or {}
    logging.info(f"Processing data with latest dates per (protocol, token): {latest_dates}")

    # Load API key and prepare base URL
    api_key = load_api_key("api_key_defillama")
    if not api_key:
        raise AirflowException("Defillama API key not found in Airflow Variables.")
    base_url = f"https://pro-api.llama.fi/{api_key}"

    tokens = TOKENS

    # Process grouped protocols across all chains
    rows = []
    for protocol, slugs in PROTOCOLS.items():
        # Map ts -> token -> total amount across child slugs
        protocol_totals = {}
        for slug in slugs:
            try:
                protocol_data = make_api_request(f"{base_url}/api/protocol/{slug}")
                chain_tvls = protocol_data.get('chainTvls', {})
                for _, chain_data in chain_tvls.items():
                    tokens_list = chain_data.get('tokens', [])
                    if not tokens_list:
                        continue
                    for token_record in tokens_list:
                        ts = token_record.get('date')
                        if not ts:
                            continue
                        token_amounts = token_record.get('tokens', {})
                        if not isinstance(token_amounts, dict):
                            continue
                        # Initialize dict for this timestamp
                        if ts not in protocol_totals:
                            protocol_totals[ts] = {t: 0.0 for t in tokens}
                        for tkn, amt in token_amounts.items():
                            if tkn in tokens:
                                try:
                                    protocol_totals[ts][tkn] = protocol_totals[ts].get(tkn, 0.0) + float(amt)
                                except Exception:
                                    # Skip if amount is not numeric
                                    continue
            except Exception as e:
                logging.error(f"Error fetching chain data for {slug}: {e}")
                raise AirflowException(f"Critical error fetching chain data for {slug}: {e}")

        # Convert aggregated totals into per-token rows
        for ts, token_map in protocol_totals.items():
            dt = datetime.fromtimestamp(ts).date()
            for tkn, total_amt in token_map.items():
                if total_amt == 0 or total_amt is None:
                    continue
                rows.append({
                    'date': dt.strftime("%Y-%m-%d"),
                    'protocol': protocol,
                    'token': tkn,
                    'amount': round(float(total_amt), 8),
                })

    if not rows:
        logging.info("No data rows generated from API response.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values(['date', 'protocol', 'token'])
    if not df.empty:
        # Exclude the most recent date present in the pull (common practice to avoid partial day)
        max_date_in_df = df['date'].max()
        df = df[df['date'] < max_date_in_df]

    # Convert date column to date objects
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Attach latest date per (protocol, token)
    def latest_for_row(row):
        key = (row['protocol'], row['token'])
        return latest_dates.get(key, datetime(2010, 1, 1).date())

    df['latest_rds_date'] = df.apply(latest_for_row, axis=1)

    # Only rows strictly newer than last stored
    df = df[df['date'] > df['latest_rds_date']]

    # Exclude recent 2 days (today and yesterday)
    cutoff_date = datetime.now().date() - timedelta(days=2)
    df = df[df['date'] <= cutoff_date]

    # Clean up temporary column
    df = df.drop(columns=['latest_rds_date'])

    logging.info(f"New records count after filtering: {len(df)}")

    if df.empty:
        logging.info("DataFrame is empty after filtering; nothing to save.")
        return

    # Log sample rows
    for i, row in df.head(10).iterrows():
        logging.info(f"Row {i}: {row.to_dict()}")

    # Save the processed data to a CSV file in temporary storage
    tmp_file = os.path.join(DATA_DIR, 'stablecoin_amounts.csv')
    df.to_csv(tmp_file, index=False)
    logging.info(f"Saved processed data to {tmp_file}")

# Task 3: Upload the CSV data to the RDS PostgreSQL table.

def upload_to_rds(**kwargs):
    tmp_file = os.path.join(DATA_DIR, 'stablecoin_amounts.csv')
    if not os.path.exists(tmp_file):
        logging.info("No CSV file found to upload. Skipping upload.")
        return
    df = pd.read_csv(tmp_file)
    if df.empty:
        logging.info("CSV file is empty. No new records to upload.")
        return
    try:
        if DO_NOT_UPLOAD:
            logging.info("DO NOT UPLOAD flag is set. Skipping actual upload.")
            return
        df.to_sql(
            TABLE_NAME,
            rds_engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logging.info(f"Successfully uploaded {len(df)} rows to {TABLE_NAME}")
    except Exception as e:
        raise AirflowException(f"Error uploading data to RDS: {e}")

# Define the DAG and tasks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60),
}

with DAG(
    'stablecoin_amounts_in_protocol_pipeline',
    default_args=default_args,
    description='Pipeline for uploading stablecoin amounts in DeFi protocols',
    schedule_interval=get_schedule_interval('stablecoin_amounts'),
    start_date=get_start_date('stablecoin_amounts'),
    catchup=False,
) as dag:

    get_latest_date_task = PythonOperator(
        task_id='get_latest_rds_dates',
        python_callable=get_latest_rds_dates,
    )

    process_data_task = PythonOperator(
        task_id='process_stablecoin_amounts',
        python_callable=process_stablecoin_amounts,
        provide_context=True,
    )

    upload_rds_task = PythonOperator(
        task_id='upload_to_rds',
        python_callable=upload_to_rds,
    )

    get_latest_date_task >> process_data_task >> upload_rds_task
