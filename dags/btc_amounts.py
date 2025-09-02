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
DATA_DIR = '/tmp/btc_amounts'
TABLE_NAME = 'btc_amounts'

# Load protocols from CSV file
def load_protocols_from_csv():
    """Load protocols dictionary from /opt/airflow/data/top_200_btc_protocols.csv"""
    protocols = {}
    csv_path = "/opt/airflow/data/top_200_btc_protocols.csv"
    
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                parent_slug = row["parentProtocolSlug"]
                child_slugs_str = row["childSlugs"]
                
                # Parse the string representation of list to actual list
                try:
                    child_slugs = ast.literal_eval(child_slugs_str)
                    if isinstance(child_slugs, list):
                        protocols[parent_slug] = child_slugs
                    else:
                        logging.warning(f"childSlugs for {parent_slug} is not a list: {child_slugs_str}")
                except (ValueError, SyntaxError) as e:
                    logging.error(f"Failed to parse childSlugs for {parent_slug}: {child_slugs_str}, error: {e}")
                    
        logging.info(f"Loaded {len(protocols)} protocols from {csv_path}")
        return protocols
        
    except Exception as e:
        logging.error(f"Failed to load protocols from {csv_path}: {e}")
        # Fallback to hardcoded protocols if CSV loading fails
        return {
            "aave": ["aave-v1", "aave-v2", "aave-v3"],
            "aerodrome": ["aerodrome-v1", "aerodrome-slipstream"],
            "pendle": ["pendle"],
            "unit": ["unit"],
            "morpho-blue": ["morpho-blue"],
            "euler": ["euler-v1", "euler-v2"],
            "pancake-swap": ["pancakeswap-amm", "pancakeswap-amm-v3", "pancakeswap-stableswap", "pancakeswap-options", "pancakeswap-amm-v1", "pancakeswap-perps"]
        }

# Load protocols dynamically from CSV
PROTOCOLS = load_protocols_from_csv()

TOKENS = ['BTC', 'WBTC', 'BTC.B', 'CBBTC', 'EBTC', 'LBTC', 'TBTC', 'WBTC.E', 'LIQUIDBERABTC',
              'SOLVBTC', 'RENBTC', '21BTC', 'SOLVBTC.BERA', 'UNIBTC', 'SWBTC']

# Define a constant for chains of interest (simplified variable name)
CHAINS = ["Sonic"]

# Ensure DATA_DIR exists
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")
    else:
        logging.info(f"Directory already exists: {directory}")

ensure_dir(DATA_DIR)

# Create RDS engine using Airflow connection 'rds_connection'
rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)

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

# Task 1: Get the latest date from the RDS table for incremental processing.
def get_latest_rds_dates(**kwargs):
    try:
        query = text(f"SELECT protocol, MAX(date) as max_date FROM {TABLE_NAME} GROUP BY protocol")
        df = pd.read_sql(query, rds_engine)
        latest_dates = {}
        if df.empty:
            latest_dates = {}
        else:
            for _, row in df.iterrows():
                protocol = row['protocol']
                if pd.isnull(row['max_date']):
                    latest_dates[protocol] = datetime(2010, 1, 1).date()
                else:
                    latest_dates[protocol] = datetime.strptime(str(row['max_date']), "%Y-%m-%d").date()
        logging.info(f"Latest dates by protocol in {TABLE_NAME}: {latest_dates}")
        return latest_dates
    except Exception as e:
        raise AirflowException(f"Error fetching latest dates from {TABLE_NAME}: {e}")


# Task 2: Fetch API data, process it, and save new records to a CSV file.
def process_btc_amounts(**kwargs):
    # Retrieve the latest dates per protocol from XCom (as a dictionary)
    ti = kwargs['ti']
    latest_dates = ti.xcom_pull(task_ids='get_latest_rds_dates')
    logging.info(f"Processing data with latest dates per protocol: {latest_dates}")

    # Load API key and prepare base URL
    api_key = load_api_key("api_key_defillama")
    if not api_key:
        raise AirflowException("Defillama API key not found in Airflow Variables.")
    base_url = f"https://pro-api.llama.fi/{api_key}"

    # Define tokens used for filtering token amounts
    tokens = TOKENS

    # --- Part A: Process grouped protocols across all chains ---
    grouped_chain_rows = []
    for protocol, slugs in PROTOCOLS.items():
        protocol_totals = {}
        for slug in slugs:
            try:
                protocol_data = make_api_request(f"{base_url}/api/protocol/{slug}")
                chain_tvls = protocol_data.get('chainTvls', {})
                for chain, chain_data in chain_tvls.items():
                    tokens_list = chain_data.get('tokens', [])
                    if tokens_list:
                        for token_record in tokens_list:
                            ts = token_record.get('date')
                            if not ts:
                                continue
                            total_tokens = sum(
                                amount for token, amount in token_record.get('tokens', {}).items() if token in tokens
                            )
                            protocol_totals[ts] = protocol_totals.get(ts, 0) + total_tokens
            except Exception as e:
                logging.error(f"Error fetching chain data for {slug}: {e}")
                raise AirflowException(f"Critical error fetching chain data for {slug}: {e}")
            
        for ts, total_btc in protocol_totals.items():
            dt = datetime.fromtimestamp(ts).date()
            grouped_chain_rows.append({
                'date': dt.strftime("%Y-%m-%d"),
                'protocol': protocol,
                'btc_amount': round(total_btc, 8)
            })

    # --- Part B: Process protocols for chains of interest ---
    protocols_data = make_api_request(f"{base_url}/api/protocols")
    # Build a list of tuples (protocol_slug, chain) if protocol has any of the chains in CHAINS
    interested_protocols = []
    for protocol in protocols_data:
        for chain in CHAINS:
            if chain in protocol.get('chainTvls', {}):
                interested_protocols.append((protocol['slug'], chain))
                break
    logging.info(f"Found {len(interested_protocols)} protocols for chains: {CHAINS}.")

    # Aggregate BTC amounts from the interested chains
    chain_date_totals = {}
    for protocol_slug, chain in interested_protocols:
        try:
            protocol_data = make_api_request(f"{base_url}/api/protocol/{protocol_slug}")
            chain_tvls = protocol_data.get('chainTvls', {})
            chain_data = chain_tvls.get(chain, {})
            tokens_data = chain_data.get('tokens', [])
            if tokens_data:
                for token_record in tokens_data:
                    ts = token_record.get('date')
                    if not ts:
                        continue
                    # Sum BTC amounts from the record for tokens that match our list
                    total_tokens = sum(
                        amount for token, amount in token_record.get('tokens', {}).items() if token in tokens
                    )
                    record_date = datetime.fromtimestamp(ts).date()
                    chain_date_totals[record_date] = chain_date_totals.get(record_date, 0) + total_tokens
        except Exception as e:
            logging.error(f"Error fetching data for protocol {protocol_slug} on chain {chain}: {e}")
            raise AirflowException(f"Critical error fetching data for protocol {protocol_slug} on chain {chain}: {e}")

    simple_rows = []
    for dt, total_btc in chain_date_totals.items():
        simple_rows.append({
            'date': dt.strftime("%Y-%m-%d"),
            'protocol': CHAINS[0].lower(),  # using the chain name as the protocol name (lowercase)
            'btc_amount': round(total_btc, 5)
        })

    # --- Combine all rows and apply filtering ---
    all_rows = simple_rows + grouped_chain_rows
    if not all_rows:
        logging.info("No data rows generated from API response.")
        return

    df = pd.DataFrame(all_rows)
    df = df.sort_values(['date', 'protocol'])
    if not df.empty:
        max_date_in_df = df['date'].max()
        df = df[df['date'] < max_date_in_df]

    # Retrieve the latest dates per protocol from RDS (as a dictionary)
    ti = kwargs['ti']
    latest_dates = ti.xcom_pull(task_ids='get_latest_rds_dates')
    logging.info(f"Processing data with latest dates per protocol: {latest_dates}")
    
    # Convert date column to date objects
    df['date'] = pd.to_datetime(df['date']).dt.date
    # For each row, add the latest date from RDS for that protocol (defaulting to 2010-01-01)
    df['latest_rds_date'] = df['protocol'].apply(lambda x: latest_dates.get(x, datetime(2010, 1, 1).date()))
    # Only process rows with a date greater than the RDS latest date for that protocol
    df = df[df['date'] > df['latest_rds_date']]
    
    # Exclude recent 2 days (today and yesterday) globally
    cutoff_date = datetime.now().date() - timedelta(days=2)
    df = df[df['date'] <= cutoff_date]
    
    # Clean up temporary column
    df = df.drop(columns=['latest_rds_date'])
    
    logging.info(f"New records count after filtering by DB latest date and excluding recent 2 days: {len(df)}")

    # Log top 10 rows for debugging
    if not df.empty:
        logging.info("Top 10 rows of processed data:")
        for i, row in df.head(10).iterrows():
            logging.info(f"Row {i}: {row.to_dict()}")
    else:
        logging.info("DataFrame is empty, no rows to display and upload.")
        return


    # Save the processed data to a CSV file in temporary storage
    tmp_file = os.path.join(DATA_DIR, 'btc_amounts.csv')
    df.to_csv(tmp_file, index=False)
    logging.info(f"Saved processed data to {tmp_file}")

# Task 3: Upload the CSV data to the RDS PostgreSQL table.
def upload_to_rds(**kwargs):
    tmp_file = os.path.join(DATA_DIR, 'btc_amounts.csv')
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
    'btc_amounts_in_protocol_pipeline',
    default_args=default_args,
    description='Pipeline for uploading BTC amounts in DeFi protocols',
    schedule_interval=get_schedule_interval('btc_amounts'),
    start_date=get_start_date('btc_amounts'),
    catchup=False,
) as dag:

    get_latest_date_task = PythonOperator(
        task_id='get_latest_rds_dates',
        python_callable=get_latest_rds_dates,
    )

    process_data_task = PythonOperator(
        task_id='process_btc_amounts',
        python_callable=process_btc_amounts,
        provide_context=True,
    )

    upload_rds_task = PythonOperator(
        task_id='upload_to_rds',
        python_callable=upload_to_rds,
    )

    get_latest_date_task >> process_data_task >> upload_rds_task
