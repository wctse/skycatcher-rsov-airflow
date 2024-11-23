from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
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

# Configuration
API_ENDPOINT = 'https://api-prod-v2.taostats.io/api/stats/history/v1'
QUERY_LIMIT = 200

# Data directory
DATA_DIR = '/opt/airflow/data/taostats/network'

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

def validate_latest_dates_file():
    latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
    if not os.path.exists(latest_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_dates_file}")
    df = pd.read_csv(latest_dates_file)
    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_dates.csv. Expected columns: table_name, max_date")

def get_page_mapping():
    mapping_file = os.path.join(DATA_DIR, 'page_mapping.json')
    if os.path.exists(mapping_file):
        with open(mapping_file, 'r') as f:
            return json.load(f)
    return {"last_date": None, "page_number": 1}

def update_page_mapping(last_date, page_number):
    mapping_file = os.path.join(DATA_DIR, 'page_mapping.json')
    mapping = {"last_date": last_date, "page_number": page_number}
    with open(mapping_file, 'w') as f:
        json.dump(mapping, f)

# Dune functions
def fetch_dune_table_dates(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Sample Query",
            query_id="4170616"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        print(f"Output from fetch_dune_table_dates: {df.to_dict('records')}")
        
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest dates to {file_path}")
        
        validate_latest_dates_file()
    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_table_dates: {e}")

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

# Task functions
def fetch_network_stats(**kwargs):
    try:
        print("Starting fetch_network_stats task")
        api_key = load_api_key("api_key_taostats")
        if not api_key:
            raise ValueError("Taostats API key not found")
        
        validate_latest_dates_file()
        
        # Read the latest dates from Dune
        latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
        latest_dates_df = pd.read_csv(latest_dates_file)
        print(f"Latest dates DataFrame: {latest_dates_df.to_dict('records')}")
        
        latest_date = latest_dates_df.loc[
            latest_dates_df['table_name'] == 'bittensor_network_stats', 
            'max_date'
        ].iloc[0]
        print(f"Latest date from Dune: {latest_date}")
        
        # Calculate one day before, but keep original filtering
        start_date = (datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"One day before latest date: {start_date}")
        
        # Get the page mapping
        page_mapping = get_page_mapping()
        current_page = page_mapping["page_number"]
        print(f"Starting from page: {current_page}")
        
        all_data = []
        has_more_data = True
        
        while has_more_data:
            print(f"\nFetching page {current_page}")
            url = f"{API_ENDPOINT}?limit={QUERY_LIMIT}&page={current_page}"
            
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
            
            if not data:
                print("Empty data received")
                break
                
            # Print complete raw data structure
            print("\nRaw data received from API:")
            for item in data[:5]:  # Print first 5 records to avoid too much output
                print("\nComplete data structure for record:")
                print(json.dumps(item, indent=2))
            print("..." if len(data) > 5 else "")
                
            # Convert timestamps to dates and filter - keep original logic
            filtered_data = []
            for item in data:
                date = item['timestamp'].split('T')[0]
                if date > start_date:
                    filtered_data.append(item)
                    
            # Print complete filtered data details
            print(f"\nFiltered data (records after {start_date}):")
            for item in filtered_data:
                print("\nComplete filtered record:")
                print(json.dumps(item, indent=2))
            
            print(f"\nFiltered to {len(filtered_data)} new records")
            
            if filtered_data:
                all_data.extend(filtered_data)
                
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
            
            # Update the page mapping
            if all_data:
                last_date = all_data[-1]['timestamp'].split('T')[0]
                update_page_mapping(last_date, current_page)
                print(f"Updated page mapping - Last date: {last_date}, Page: {current_page}")
        else:
            # Get latest date from Dune
            latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
            latest_dates_df = pd.read_csv(latest_dates_file)
            latest_dune_date = latest_dates_df.loc[
                latest_dates_df['table_name'] == 'bittensor_network_stats', 
                'max_date'
            ].iloc[0]
            
            print(f"Latest date in Dune: {latest_dune_date}")
            print("Latest date in fetched data: No new data available")
            print("No new data to save")
            
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
        processed_data = {}
        for date, items in sorted(dates_data.items())[:-1]:  # Skip the last (potentially incomplete) day
            # Take the earliest entry for each complete day
            item = items[0]
            
            # Get date and increment by one day to align with data reporting
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime('%Y-%m-%d')
            
            # Only keep the first entry for each date
            if date_str not in processed_data:
                processed_record = {
                    'date': date_str,
                    'block_number': item['block_number'],
                    'issued': float(item['issued']) / 1e9,
                    'staked': float(item['staked']) / 1e9,
                    'accounts': item['accounts'],
                    'active_accounts': item['active_accounts'],
                    'balance_holders': item['balance_holders'],
                    'active_balance_holders': item['active_balance_holders'],
                    'extrinsics': item['extrinsics'],
                    'transfers': item['transfers'],
                    'subnets': item['subnets'],
                    'subnet_registration_cost': float(item['subnet_registration_cost']) / 1e9
                }
                processed_data[date_str] = processed_record
                if DEV_MODE:
                    print(f"Processed record for {date_str}: {processed_record}")
        
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(list(processed_data.values()))
        csv_file = os.path.join(DATA_DIR, 'stats_history.csv')
        df.to_csv(csv_file, index=False)
        
        print(f"Processed and saved {len(df)} entries to {csv_file}")
        if DEV_MODE:
            print("DataFrame head:")
            print(df.head().to_string())
            print("\nDataFrame info:")
            print(df.info())
        
    except Exception as e:
        print(f"Error in process_network_stats: {str(e)}")
        raise AirflowException(f"Error in process_network_stats: {e}")

def upload_to_dune(**kwargs):
    # Override DEV_MODE to be True for this function
    DEV_MODE = False
    
    try:
        # Read the processed CSV
        csv_file = os.path.join(DATA_DIR, 'stats_history.csv')
        df = pd.read_csv(csv_file)
        
        if DEV_MODE:
            print(f"DEV MODE: Data to be uploaded:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
        else:
            # Upload to Dune
            response = insert_df_to_dune(df, 'bittensor_network_stats')
            print(f"Uploaded data to Dune. Response: {response}")
        
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")

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
    'taostats_network_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing Taostats network statistics',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
)

get_latest_dates_task = PythonOperator(
    task_id='get_latest_dates',
    python_callable=fetch_dune_table_dates,
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

# Set up task dependencies
get_latest_dates_task >> fetch_network_stats_task >> process_network_stats_task >> upload_to_dune_task