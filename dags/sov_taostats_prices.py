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
import os
from airflow.utils.dates import days_ago
import time
from requests.exceptions import RequestException

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# Global Development Mode Flag
DEV_MODE = False  # Set to False for production
DO_NOT_UPLOAD = False

# Configuration
API_ENDPOINT = 'https://api.taostats.io/api/price/history/v1'
QUERY_LIMIT = 200

# Data directory
DATA_DIR = '/tmp/taostats/prices'
TABLE_NAME = 'bittensor_prices'

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

def validate_latest_rds_dates_file():
    latest_rds_dates_file = os.path.join(DATA_DIR, 'latest_rds_dates.csv')

    if not os.path.exists(latest_rds_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_rds_dates_file}")
    
    df = pd.read_csv(latest_rds_dates_file)

    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_rds_dates.csv. Expected columns: table_name, max_date")

# RDS functions
def fetch_rds_table_dates(**kwargs):
    try:        
        query = text(f"""
            SELECT '{TABLE_NAME}' as table_name, COALESCE(MAX(date)::text, '2020-01-01') as max_date 
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
    
def parse_latest_dates(delta=0):
    validate_latest_rds_dates_file()
    latest_rds_dates_file = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
    latest_rds_dates_df = pd.read_csv(latest_rds_dates_file)

    rds_date = latest_rds_dates_df.loc[
        latest_rds_dates_df['table_name'] == TABLE_NAME,
        'max_date'
    ].iloc[0]

    # Convert string date to datetime object
    rds_date = datetime.strptime(rds_date, '%Y-%m-%d') + timedelta(days=delta)

    print(f"RDS date: {rds_date}")

    return {
        'rds_date': rds_date,
        'min_of_max_dates': rds_date
    }

# Task functions
def fetch_price_stats(**kwargs):  
    try:
        print("Starting fetch_price_stats task")
        
        api_key = load_api_key("api_key_taostats")
        if not api_key:
            raise ValueError("Taostats API key not found")
        
        latest_dates = parse_latest_dates(-1) # The data is usually timestamped at 23:59 UTC, so we need to fetch the previous day
        start_date = (latest_dates['min_of_max_dates'])
        
        # Query API for earliest available data
        print("Querying API for earliest available data...")
        url = f"{API_ENDPOINT}?asset=TAO&limit=1&order=timestamp_asc"
        response = make_api_request(
            url,
            method='GET',
            headers={'Authorization': api_key}
        )
        
        if response and 'data' in response and response['data']:
            earliest_record = response['data'][0]
            earliest_date_str = earliest_record['created_at'].split('T')[0]
            earliest_api_date = datetime.strptime(earliest_date_str, '%Y-%m-%d') - timedelta(days=1)
            print(f"Earliest available data in API is from {earliest_date_str}")
            
            # Use the later of the two dates (don't fetch data that doesn't exist in API)
            if earliest_api_date > start_date:
                print(f"Adjusting start_date from {start_date} to {earliest_api_date} (API data starts later)")
                start_date = earliest_api_date
        else:
            print("Could not determine earliest available data from API. Proceeding with RDS date.")
        
        end_date = datetime.now() - timedelta(days=1)  # Exclude today as it's incomplete

        # Generate list of dates that need data
        dates_to_fetch = []
        current_date = start_date + timedelta(days=1)
        while current_date <= end_date:
            dates_to_fetch.append(current_date)
            current_date += timedelta(days=1)

        if not dates_to_fetch:
            print(f"No dates to fetch. Start date: {start_date}. End date: {end_date}. Exiting fetch_price_stats.")
            return

        print(f"Fetching price stats for {len(dates_to_fetch)} dates from {dates_to_fetch[0]} to {dates_to_fetch[-1]}")
        
        if DEV_MODE:
            print("DEV_MODE: Fetching only first 10 dates")
            dates_to_fetch = dates_to_fetch[:10]
        
        all_data = []
        
        # Fetch data for each date individually
        for date in dates_to_fetch:
            # Query for a 24-hour window for this specific date
            day_start = date.replace(hour=0, minute=0, second=0)
            day_end = date.replace(hour=23, minute=59, second=59)
            start_timestamp = int(day_start.timestamp())
            end_timestamp = int(day_end.timestamp())
            
            print(f"\nFetching data for {date.strftime('%Y-%m-%d')}")
            url = f"{API_ENDPOINT}?asset=TAO&limit=1&timestamp_start={start_timestamp}&timestamp_end={end_timestamp}&order=timestamp_asc"
            
            response = make_api_request(
                url,
                method='GET',
                headers={'Authorization': api_key}
            )
            
            if not response or 'data' not in response:
                print(f"No data in response for {date.strftime('%Y-%m-%d')}")
                continue
                
            data = response['data']
            if data:
                print(f"Received {len(data)} record(s) for {date.strftime('%Y-%m-%d')}")
                all_data.extend(data)
            else:
                print(f"No data available for {date.strftime('%Y-%m-%d')}")

        if all_data:
            # Save the raw data
            ensure_dir(DATA_DIR)
            raw_file = os.path.join(DATA_DIR, 'price_history.json')
            with open(raw_file, 'w') as f:
                json.dump(all_data, f)
            print(f"Saved {len(all_data)} records to {raw_file}")

        else:
            print(f"No new data to save. Start date: {start_date}. End date: {end_date}. Exiting fetch_price_stats.")
            
    except Exception as e:
        print(f"Error in fetch_price_stats: {str(e)}")
        raise AirflowException(f"Error in fetch_price_stats: {e}")

def process_price_stats(**kwargs):
    try:
        print("Starting process_price_stats task")
        
        # Read the raw data
        raw_file = os.path.join(DATA_DIR, 'price_history.json')
        with open(raw_file, 'r') as f:
            raw_data = json.load(f)
        
        print(f"Read {len(raw_data)} records from {raw_file}")
        latest_dates = parse_latest_dates()
        
        # Process each record (already 1 per day from fetch)
        processed_data = []
        for item in raw_data:
            # Get date and increment by one day to align with data reporting
            date = item['created_at'].split('T')[0]
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime('%Y-%m-%d')
            
            processed_record = {
                'date': date_str,
                'circulating_supply': float(item['circulating_supply']),
                'price': float(item['price']),
                'market_cap': float(item['market_cap']),
                'fully_diluted_market_cap': float(item['fully_diluted_market_cap'])
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

        # Filter for RDS
        df_rds = df[df['date'] > latest_dates['rds_date']].copy()

        processed_dir = os.path.join(DATA_DIR, 'processed')
        ensure_dir(processed_dir)
        
        df_rds.to_csv(os.path.join(processed_dir, 'price_history_rds.csv'), index=False)
        
        print(f"Processed and saved {len(df_rds)} records for RDS from {df_rds['date'].min()} to {df_rds['date'].max()}")
        
    except Exception as e:
        raise AirflowException(f"Error in process_price_stats: {e}")
    
def upload_to_rds(**kwargs):
    try:
        task_instance = kwargs['task_instance']
        no_data = task_instance.xcom_pull(key='no_data', task_ids='process_price_stats')
        
        if no_data:
            print("No data was processed. Skipping RDS upload.")
            return
        
        processed_file = os.path.join(DATA_DIR, 'processed', 'price_history_rds.csv')
        
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
    'sov_taostats_prices',
    default_args=default_args,
    description=get_dag_config('sov_taostats_prices')['description'],
    schedule_interval=get_schedule_interval('sov_taostats_prices'),
    start_date=get_start_date('sov_taostats_prices'),
    catchup=False
)

fetch_rds_table_dates_task = PythonOperator(
    task_id='fetch_rds_table_dates',
    python_callable=fetch_rds_table_dates,
    dag=dag,
)

fetch_price_stats_task = PythonOperator(
    task_id='fetch_price_stats',
    python_callable=fetch_price_stats,
    dag=dag,
)

process_price_stats_task = PythonOperator(
    task_id='process_price_stats',
    python_callable=process_price_stats,
    dag=dag,
)

upload_to_rds_task = PythonOperator(
    task_id='upload_to_rds',
    python_callable=upload_to_rds,
    dag=dag,
)

# Set up task dependencies
fetch_rds_table_dates_task >> fetch_price_stats_task >> process_price_stats_task >> upload_to_rds_task