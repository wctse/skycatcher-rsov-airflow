from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
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
API_ENDPOINT = "https://api.artemisxyz.com/data"
METRICS = ['price', 'mc', 'fdv']
ASSETS = [
    'aptos',
    'avalanche',
    # 'bittensor' # Bittensor is not availalbe on Artemis, as well as we are using mcap for it
    # 'binance', # ignoring BNB chain for now
    'celestia',
    'ethereum',
    'near',
    'sei',
    'solana',
    'sui',
    'ton'
]

# Data directory
DATA_DIR = '/tmp/artemis_data'
TABLE_NAME = 'msov_asset_metrics'

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

# Reuse common utility functions
def load_api_key(key_name):
    try:
        return Variable.get(key_name)
    except KeyError:
        print(f"API key '{key_name}' not found. Please set it in Airflow Variables.")
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
            response = requests.request(method, url, headers=headers, json=data, verify=False)
            make_api_request.last_request_time = time.time()
            
            print(f"URL: {url}")
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
                print(f"Error occurred: {response.status_code}")
            return None
            
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

def fetch_dune_table_dates(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Sample Query",
            query_id="4170616" #"4170616" if not DEV_MODE else "4226299"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        print(f"Output from fetch_dune_table_dates: {df.to_dict('records')}")
        
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest dates to {file_path}")
        print(f"Latest Dune dates: {df.to_string()}")
        
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

# Task-specific functions
def fetch_artemis_data(**kwargs):
    try:
        api_key = load_api_key("api_key_artemis")
        if not api_key:
            raise ValueError("Artemis API key not found")

        latest_dates = parse_latest_dates(1)
        
        start_date = (latest_dates['min_of_max_dates']).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')

        if datetime.strptime(start_date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d'):
            print("Start date is after end date, no new data to fetch")
            return
            
        metrics = ','.join(METRICS)
        artemisids = ','.join(ASSETS[:2] if DEV_MODE else ASSETS)
        
        print(f"Fetching data from {start_date} to {end_date}")
        url = f"{API_ENDPOINT}/{metrics}?APIKey={api_key}&artemisIds={artemisids}&startDate={start_date}&endDate={end_date}"
        
        response = make_api_request(url)
        
        if not response or 'data' not in response or 'artemis_ids' not in response['data']:
            raise AirflowException("Invalid response format from Artemis API")
            
        # Save raw data
        ensure_dir(DATA_DIR)
        raw_file = os.path.join(DATA_DIR, 'raw_data.json')
        with open(raw_file, 'w') as f:
            json.dump(response, f)
            
        print(f"Saved raw data to {raw_file}")
        
    except Exception as e:
        raise AirflowException(f"Error in fetch_artemis_data: {e}")

def process_artemis_data(**kwargs):
    try:
        raw_file = os.path.join(DATA_DIR, 'raw_data.json')
        with open(raw_file, 'r') as f:
            raw_data = json.load(f)
            
        artemis_data = raw_data['data']['artemis_ids']
        latest_dates = parse_latest_dates()
        
        # Transform data into flat format
        records = []
        current_date = datetime.now().date()
        for asset, metrics in artemis_data.items():
            # Get all dates from the price data
            dates = [entry['date'] for entry in metrics['price']]
            
            # For each date, create a row with all metrics
            for date in dates:
                date_obj = datetime.strptime(date, '%Y-%m-%d').date()

                # Sometimes Artemis has incomplete data for the previous day with N/A in mcap, so we skip it and limit the lag to 2 days
                if (current_date - date_obj).days >= 2:
                    price = next((entry['val'] for entry in metrics['price'] if entry['date'] == date), None)
                    mc = next((entry['val'] for entry in metrics['mc'] if entry['date'] == date), None)
                    fdv = next((entry['val'] for entry in metrics['fdv'] if entry['date'] == date), None)
                    
                    # Handle 0.0 values in market cap (like in the NEAR example)
                    if mc == 0.0:
                        mc = None
                        
                    records.append({
                        'date': date,
                        'asset': asset,
                        'price': price,
                        'mc': mc,
                        'fdv': fdv
                    })

                else:
                    print(f"Skipping date {date} for asset {asset} because it's too recent")
        
        # Create DataFrame
        df = (pd.DataFrame(records)
              .dropna(subset=['price', 'mc', 'fdv'], how='all')
              .sort_values(['date', 'asset'])
              .reset_index(drop=True))
        
        # Convert date strings to datetime.date objects for comparison
        df['date'] = pd.to_datetime(df['date'])
        
        # Create two filtered versions
        df_dune = df[df['date'] > latest_dates['dune_date']].copy()
        df_rds = df[df['date'] > latest_dates['rds_date']].copy()
        
        # Save processed data
        processed_dir = os.path.join(DATA_DIR, 'processed')
        ensure_dir(processed_dir)
        
        # Save both versions
        df_dune.to_csv(os.path.join(processed_dir, 'asset_metrics_dune.csv'), index=False)
        df_rds.to_csv(os.path.join(processed_dir, 'asset_metrics_rds.csv'), index=False)
        
        print(f"Processed and saved {len(df_dune)} records for Dune from {df_dune['date'].min()} to {df_dune['date'].max()}")
        print(f"Processed and saved {len(df_rds)} records for RDS from {df_rds['date'].min()} to {df_rds['date'].max()}")
        
    except Exception as e:
        raise AirflowException(f"Error in process_artemis_data: {e}")

def upload_to_dune(**kwargs):
    try:
        processed_dir = os.path.join(DATA_DIR, 'processed')
        processed_file = os.path.join(processed_dir, 'asset_metrics_dune.csv')
        
        if not os.path.exists(processed_file):
            raise AirflowException(f"Processed file not found: {processed_file}")
            
        df = pd.read_csv(processed_file)
        
        if DO_NOT_UPLOAD:
            print("DO NOT UPLOAD: Data to be uploaded:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
        else:
            response = insert_df_to_dune(df, TABLE_NAME)
            print(f"Uploaded data to Dune. Response: {response}")
            
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")
    
def upload_to_rds(**kwargs):
    try:
        processed_dir = os.path.join(DATA_DIR, 'processed')
        processed_file = os.path.join(processed_dir, 'asset_metrics_rds.csv')
        
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
    'artemis_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing Artemis price, market cap and fdv data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
)

get_latest_dune_dates_task = PythonOperator(
    task_id='get_latest_dune_dates',
    python_callable=fetch_dune_table_dates,
    dag=dag,
)

get_latest_rds_dates_task = PythonOperator(
    task_id='get_latest_rds_dates',
    python_callable=fetch_rds_table_dates,
    dag=dag,
)

fetch_artemis_data_task = PythonOperator(
    task_id='fetch_artemis_data',
    python_callable=fetch_artemis_data,
    dag=dag,
)

process_artemis_data_task = PythonOperator(
    task_id='process_artemis_data',
    python_callable=process_artemis_data,
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
[get_latest_dune_dates_task, get_latest_rds_dates_task] >> fetch_artemis_data_task >> process_artemis_data_task >> [upload_to_dune_task, upload_to_rds_task]