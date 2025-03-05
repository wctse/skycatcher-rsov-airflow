from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import os
from airflow.utils.dates import days_ago
import time
from requests.exceptions import RequestException

# Global Development Mode Flag
DEV_MODE = False  # Set to False for production
DO_NOT_UPLOAD = False

# Configuration
API_ENDPOINT = 'https://api.taostats.io/api/stats/history/v1'
QUERY_LIMIT = 200

# Data directory
DATA_DIR = '/tmp/taostats/network'
TABLE_NAME = 'bittensor_network_stats'

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

# Utility functions
def load_api_key(key_name):
    try:
        return Variable.get(key_name)
    except KeyError:
        print(f"API key '{key_name}' not found. Please set it in Airflow Variables.")
        return None

def make_api_request(url, method='GET', headers=None, data=None, max_retries=3, retry_delay=60):
    headers = headers or {}
    headers['Content-Type'] = 'application/json'
    
    last_request_time = getattr(make_api_request, 'last_request_time', 0)
    
    for attempt in range(max_retries):
        current_time = time.time()
        time_since_last_request = current_time - last_request_time
        
        if time_since_last_request < 1.5:
            time.sleep(1.5 - time_since_last_request)
        
        try:
            response = requests.request(method, url, json=data, headers=headers, verify=False)
            make_api_request.last_request_time = time.time()
            
            if method == 'GET':
                print(f"URL: {url}")
            else:
                print(f"Query: {data}")
            print(f"Response status code: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            
            elif response.status_code == 429:
                if attempt < max_retries - 1:
                    print(f"Rate limit hit. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print("Max retries reached for rate limit.")
            else:
                raise AirflowException(f"Error occurred: {response.status_code}")
            
        except RequestException as e:
            print(f"API request failed: {e}")
            print(f"Response content: {e.response.text if e.response else 'No response'}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached.")
                return None

    return None

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")

def validate_latest_dune_dates_file():
    latest_dune_dates_file = os.path.join(DATA_DIR, 'latest_dune_dates.csv')

    if not os.path.exists(latest_dune_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_dune_dates_file}")
    
    df = pd.read_csv(latest_dune_dates_file)

    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_dune_dates.csv. Expected columns: table_name, max_date")
    
def validate_latest_rds_dates_file():
    latest_rds_dates_file = os.path.join(DATA_DIR, 'latest_rds_dates.csv')

    if not os.path.exists(latest_rds_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_rds_dates_file}")
    
    df = pd.read_csv(latest_rds_dates_file)

    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_rds_dates.csv. Expected columns: table_name, max_date")

# Dune functions
def fetch_dune_table_dates(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Sample Query",
            query_id="4170616" # "4170616" if not DEV_MODE else "4226299"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        print(f"Output from fetch_dune_table_dates: {df.to_dict('records')}")
        
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest dates to {file_path}")
        
        validate_latest_dune_dates_file()

    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_table_dates: {e}")

def fetch_rds_table_dates(**kwargs):
    try:        
        query = text(f"""
            SELECT '{TABLE_NAME}' as table_name, MAX(date) as max_date 
            FROM {TABLE_NAME}
        """)
        
        df = pd.read_sql(query, rds_engine)
        
        # Save RDS dates to a separate file
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved RDS dates to {file_path}")
        print(f"Latest RDS dates: {df.to_string()}")
        
        validate_latest_rds_dates_file()

    except Exception as e:
        raise AirflowException(f"Error in fetch_rds_table_dates: {e}")

def insert_df_to_dune(df, table_name):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        csv_data = df.to_csv(index=False)
        csv_bytes = csv_data.encode('utf-8')
        response = dune.insert_table(
            namespace="sc_research",
            table_name=table_name,
            data=csv_bytes,
            content_type="text/csv"
        )
        return response
    
    except Exception as e:
        raise AirflowException(f"Error in insert_df_to_dune: {e}")
    
def parse_latest_dates(delta=0):
    validate_latest_dune_dates_file()
    latest_dune_dates_file = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
    latest_dune_dates_df = pd.read_csv(latest_dune_dates_file)

    validate_latest_rds_dates_file()
    latest_rds_dates_file = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
    latest_rds_dates_df = pd.read_csv(latest_rds_dates_file)

    dune_date = latest_dune_dates_df.loc[
        latest_dune_dates_df['table_name'] == TABLE_NAME,
        'max_date'
    ].iloc[0]

    rds_date = latest_rds_dates_df.loc[
        latest_rds_dates_df['table_name'] == TABLE_NAME,
        'max_date'
    ].iloc[0]

    # Convert string dates to datetime objects for comparison
    dune_date = datetime.strptime(dune_date, '%Y-%m-%d') + timedelta(days=delta)
    rds_date = datetime.strptime(rds_date, '%Y-%m-%d') + timedelta(days=delta)

    print(f"Dune date: {dune_date}")
    print(f"RDS date: {rds_date}")
    
    # Get the earliest date to ensure we don't miss any data
    min_of_max_dates = min(dune_date, rds_date)

    return {
        'dune_date': dune_date,
        'rds_date': rds_date,
        'min_of_max_dates': min_of_max_dates
    }

# Task functions
def fetch_network_stats(**kwargs):  
    try:
        print("Starting fetch_network_stats task")
        
        api_key = load_api_key("api_key_taostats")
        if not api_key:
            raise ValueError("Taostats API key not found")
        
        latest_dates = parse_latest_dates(-1) # The data is usually timestamped at 23:59 UTC, so we need to fetch the previous day
        start_date = (latest_dates['min_of_max_dates'])
        start_timestamp = int(start_date.timestamp())

        print(f"Fetching network stats from {start_date}")
        
        all_data = []
        has_more_data = True
        current_page = 1
        
        while has_more_data:
            print(f"\nFetching page {current_page} with unix timestamp ({start_timestamp})")
            url = f"{API_ENDPOINT}?limit={QUERY_LIMIT}&page={current_page}&timestamp_start={start_timestamp}"
            
            response = make_api_request(
                url,
                method='GET',
                headers={'Authorization': api_key}
            )
            
            if not response or 'data' not in response:
                print("No data in response")
                break
                
            data = response['data']
            print(f"Received {len(data)} records")
            print(f"Sample of raw data: {json.dumps(data[-1], indent=2)}")
            
            if not data:
                print("Empty data received")
                break
                
            filtered_data = [item for item in data if datetime.strptime(item['timestamp'].split('T')[0], '%Y-%m-%d') > start_date]
            if filtered_data:
                all_data.extend(filtered_data)
            
            print(f"\nFiltered and processed to {len(filtered_data)} new records")
            
            # Check if there's more data to fetch
            if len(data) < QUERY_LIMIT or (DEV_MODE and current_page == 1):
                print("No more data to fetch" if len(data) < QUERY_LIMIT else "DEV_MODE: Stopping after first page")
                has_more_data = False
            else:
                current_page += 1

        if all_data:
            # Save the raw data
            ensure_dir(DATA_DIR)
            raw_file = os.path.join(DATA_DIR, 'stats_history.json')
            with open(raw_file, 'w') as f:
                json.dump(all_data, f)
            print(f"Saved {len(all_data)} records to {raw_file}")

        else:
            print(f"No new data to save. Start date: {start_date}. Current date: {datetime.now().strftime('%Y-%m-%d')}. Exiting fetch_network_stats.")
            
    except Exception as e:
        print(f"Error in fetch_network_stats: {str(e)}")
        raise AirflowException(f"Error in fetch_network_stats: {e}")

def process_network_stats(**kwargs):
    try:
        print("Starting process_network_stats task")
        
        # Read the raw data
        raw_file = os.path.join(DATA_DIR, 'stats_history.json')
        with open(raw_file, 'r') as f:
            raw_data = json.load(f)
        
        print(f"Read {len(raw_data)} records from {raw_file}")
        latest_dates = parse_latest_dates()
        
        if DEV_MODE:
            print("DEV_MODE: Processing only first page of results")
            raw_data = raw_data[:QUERY_LIMIT]
        
        # Group data by date to find complete days
        dates_data = {}
        for item in raw_data:
            date = item['timestamp'].split('T')[0]
            if date not in dates_data:
                dates_data[date] = []
            dates_data[date].append(item)

        # Process the data, skipping the latest incomplete day
        processed_data = []
        for date, items in sorted(dates_data.items())[:-1]:  # Skip the last (potentially incomplete) day
            # Take the earliest entry for each complete day
            print(f"Processing item: {items}")
            item = items[0]
            
            # Get date and increment by one day to align with data reporting
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime('%Y-%m-%d')

            
            processed_record = {
                'date': date_str,
                'block_number': item['block_number'],
                'issued': float(item['issued']) / 1e9,
                'staked': float(item['staked']) / 1e9,
                'accounts': item.get('accounts', 0),
                'active_accounts': item.get('active_accounts', 0),
                'balance_holders': item.get('balance_holders', 0),
                'active_balance_holders': item.get('active_balance_holders', 0),
                'extrinsics': item.get('extrinsics', 0),
                'transfers': item.get('transfers', 0),
                'subnets': item.get('subnets', 0),
                'subnet_registration_cost': float(item.get('subnet_registration_cost', 0)) / 1e9
            }
            processed_data.append(processed_record)
        
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(processed_data)

        if df.empty:
            task_instance = kwargs['task_instance']
            task_instance.xcom_push(key='no_data', value=True)
            print("No data to process. Skipping upload.")
            return

        print(f"Processed data: {df.to_string(index=False)}")
        df['date'] = pd.to_datetime(df['date'])

        # Create two filtered versions
        df_dune = df[df['date'] > latest_dates['dune_date']].copy()
        df_rds = df[df['date'] > latest_dates['rds_date']].copy()

        processed_dir = os.path.join(DATA_DIR, 'processed')
        ensure_dir(processed_dir)
        
        df_dune.to_csv(os.path.join(processed_dir, 'stats_history_dune.csv'), index=False)
        df_rds.to_csv(os.path.join(processed_dir, 'stats_history_rds.csv'), index=False)
        
        print(f"Processed and saved {len(df_dune)} records for Dune from {df_dune['date'].min()} to {df_dune['date'].max()}")
        print(f"Processed and saved {len(df_rds)} records for RDS from {df_rds['date'].min()} to {df_rds['date'].max()}")
        
    except Exception as e:
        raise AirflowException(f"Error in process_network_stats: {e}")

def upload_to_dune(**kwargs):
    try:
        task_instance = kwargs['task_instance']
        no_data = task_instance.xcom_pull(key='no_data', task_ids='process_network_stats')
        
        if no_data:
            print("No data was processed. Skipping Dune upload.")
            return

        csv_file = os.path.join(DATA_DIR, 'processed', 'stats_history_dune.csv')
        df = pd.read_csv(csv_file)
        
        if DO_NOT_UPLOAD:
            print(f"DO NOT UPLOAD: Data to be uploaded:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
            
        else:
            response = insert_df_to_dune(df, 'bittensor_network_stats')
            print(f"Uploaded data to Dune. Response: {response}")
        
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")
    
def upload_to_rds(**kwargs):
    try:
        task_instance = kwargs['task_instance']
        no_data = task_instance.xcom_pull(key='no_data', task_ids='process_network_stats')
        
        if no_data:
            print("No data was processed. Skipping RDS upload.")
            return
        
        processed_file = os.path.join(DATA_DIR, 'processed', 'stats_history_rds.csv')
        
        if not os.path.exists(processed_file):
            raise AirflowException(f"Processed file not found: {processed_file}")
            
        df = pd.read_csv(processed_file)
        
        if DO_NOT_UPLOAD:
            print("DO NOT UPLOAD: Data to be uploaded to RDS:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
            return

        df.to_sql(
            TABLE_NAME,
            rds_engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"Successfully uploaded {len(df)} rows to RDS table {TABLE_NAME}")
            
    except Exception as e:
        raise AirflowException(f"Error in upload_to_rds: {e}")

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

dag = DAG(
    'sov_taostats_network_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing Taostats network statistics',
    schedule_interval=timedelta(days=3),
    start_date=datetime(2025, 2, 26, 3, 0, 0),
    catchup=False
)

fetch_dune_table_dates_task = PythonOperator(
    task_id='fetch_dune_table_dates',
    python_callable=fetch_dune_table_dates,
    dag=dag,
)

fetch_rds_table_dates_task = PythonOperator(
    task_id='fetch_rds_table_dates',
    python_callable=fetch_rds_table_dates,
    dag=dag,
)

fetch_network_stats_task = PythonOperator(
    task_id='fetch_network_stats',
    python_callable=fetch_network_stats,
    dag=dag,
)

process_network_stats_task = PythonOperator(
    task_id='process_network_stats',
    python_callable=process_network_stats,
    dag=dag,
)

upload_to_dune_task = PythonOperator(
    task_id='upload_to_dune',
    python_callable=upload_to_dune,
    dag=dag,
)

upload_to_rds_task = PythonOperator(
    task_id='upload_to_rds',
    python_callable=upload_to_rds,
    dag=dag,
)

# Set up task dependencies
[fetch_dune_table_dates_task, fetch_rds_table_dates_task] >> fetch_network_stats_task >> process_network_stats_task >> [upload_to_dune_task, upload_to_rds_task]