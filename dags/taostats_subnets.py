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
TEST_MODE = False  # Set to True to skip data fetching tasks and use saved data

# Configuration
API_ENDPOINTS = {
    'subnet_list': 'https://api.taostats.io/api/subnet/latest/v1',
    'emission_history': 'https://api.taostats.io/api/subnet/history/v1'
}
QUERY_LIMIT = 200

# Data directory
DATA_DIR = '/opt/airflow/data/taostats/subnet'
SUBNET_HISTORY_DIR = os.path.join(DATA_DIR, 'subnet_history')

# Reused utility functions from existing DAGs
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

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")
    else:
        print(f"Directory already exists: {directory}")

# Reused Dune functions
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

# Validation functions
def validate_latest_dates_file():
    latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
    if not os.path.exists(latest_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_dates_file}")
    df = pd.read_csv(latest_dates_file)
    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_dates.csv. Expected columns: table_name, max_date")

def validate_subnet_list_file():
    subnet_list_file = os.path.join(DATA_DIR, 'subnet_list.json')
    if not os.path.exists(subnet_list_file):
        raise AirflowException(f"Subnet list file not found: {subnet_list_file}")
    with open(subnet_list_file, 'r') as f:
        data = json.load(f)
    if not isinstance(data, dict) or 'data' not in data:
        raise AirflowException("Invalid subnet list format. Expected object with 'data' array.")
    if not isinstance(data['data'], list):
        raise AirflowException("Invalid subnet list format. 'data' should be an array.")

def validate_subnet_history_files(netuids):
    if not os.path.exists(SUBNET_HISTORY_DIR):
        raise AirflowException(f"Subnet history directory not found: {SUBNET_HISTORY_DIR}")
    
    if DEV_MODE:
        netuids = netuids[:2]
        
    for netuid in netuids:
        file_path = os.path.join(SUBNET_HISTORY_DIR, f'subnet_{netuid}.json')
        if not os.path.exists(file_path):
            raise AirflowException(f"Subnet history file not found: {file_path}")
        with open(file_path, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise AirflowException(f"Invalid format in {file_path}. Expected array of objects.")
        if data and not all(isinstance(item, dict) and 'timestamp' in item and 'emission' in item for item in data):
            raise AirflowException(f"Invalid data format in {file_path}. Missing required fields.")

# Helper functions
def get_last_processed_timestamp(netuid):
    file_path = os.path.join(SUBNET_HISTORY_DIR, f'subnet_{netuid}.json')
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                if data:
                    return data[-1]['timestamp']
    except Exception as e:
        print(f"Error reading last timestamp for subnet {netuid}: {e}")
    return None

def adjust_date(timestamp):
    date = datetime.strptime(timestamp.split('.')[0].rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
    date += timedelta(days=1)
    return date.strftime('%Y-%m-%d')

# Task functions
def fetch_subnet_list(**kwargs):
    try:
        api_key = load_api_key("api_key_taostats")
        if not api_key:
            raise ValueError("TaoStats API key not found")

        response = make_api_request(
            API_ENDPOINTS['subnet_list'],
            headers={'Authorization': api_key}
        )
        
        if response is None:
            raise AirflowException("Failed to fetch subnet list")
            
        ensure_dir(DATA_DIR)
        subnet_list_file = os.path.join(DATA_DIR, 'subnet_list.json')
        with open(subnet_list_file, 'w') as f:
            json.dump(response, f, indent=2)
            
        print(f"Fetched subnet list with {len(response['data'])} subnets")
        validate_subnet_list_file()
    except Exception as e:
        raise AirflowException(f"Error in fetch_subnet_list: {e}")
    
def parse_timestamp(timestamp_str, timestamp_formats, return_datetime=False):
    """
    Attempts to parse a timestamp string in multiple formats and returns the datetime or unix timestamp.
    Raises ValueError if none of the formats match.
    
    Args:
        timestamp_str (str): Timestamp string to parse
        timestamp_formats (list or str): List of timestamp formats to try, or a single format string
        return_datetime (bool): Whether to return a datetime object or a unix timestamp
    Returns:
        datetime or int: Datetime object or unix timestamp
        
    Raises:
        ValueError: If timestamp cannot be parsed in any supported format
    """

    if type(timestamp_formats) == str:
        timestamp_formats = [timestamp_formats]
    
    for fmt in timestamp_formats:
        try:
            if return_datetime:
                return datetime.strptime(timestamp_str.strip(), fmt)
            else:
                return int(datetime.strptime(timestamp_str.strip(), fmt).timestamp())
        
        except ValueError:
            continue
            
    raise ValueError(f"Timestamp '{timestamp_str}' does not match any input format: {timestamp_formats}")


def fetch_subnet_histories(**kwargs):
    try:
        validate_subnet_list_file()
        
        api_key = load_api_key("api_key_taostats")
        if not api_key:
            raise ValueError("TaoStats API key not found")

        subnet_list_file = os.path.join(DATA_DIR, 'subnet_list.json')
        with open(subnet_list_file, 'r') as f:
            subnet_data = json.load(f)

        ensure_dir(SUBNET_HISTORY_DIR)
        
        latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
        latest_dates_df = pd.read_csv(latest_dates_file)
        latest_date = latest_dates_df.loc[
            latest_dates_df['table_name'] == 'bittensor_subnets_stats', 
            'max_date'
        ].iloc[0]
        
        # Format the start date to include time component
        start_timestamp = parse_timestamp(latest_date, "%Y-%m-%d")
        timestamp_formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
        
        subnets_to_process = subnet_data['data'] if not DEV_MODE else subnet_data['data'][:2]
        
        for subnet in subnets_to_process:
            netuid = subnet['netuid']
            print(f"Processing subnet {netuid}")
            
            file_path = os.path.join(SUBNET_HISTORY_DIR, f'subnet_{netuid}.json')
            
            new_history_data = []
            current_start = start_timestamp
            has_more_data = True
            
            while has_more_data:
                url = f"{API_ENDPOINTS['emission_history']}?netuid={netuid}&limit={QUERY_LIMIT}&timestamp_start={current_start}&order=timestamp_asc"
                print(f"Fetching data for subnet {netuid} after {current_start}")
                
                response = make_api_request(url, headers={'Authorization': api_key})
                
                if not response or 'data' not in response:
                    break
                    
                new_items = response['data']

                if not new_items:
                    break
                    
                new_history_data.extend(new_items)
                
                if len(new_items) < QUERY_LIMIT:
                    has_more_data = False
                else:
                    # Update current_start to the timestamp of the last item
                    current_start = parse_timestamp(new_items[-1]['timestamp'], timestamp_formats, return_datetime=False)
            
            # Sort new data by timestamp before saving
            new_history_data.sort(key=lambda x: x['timestamp'])
            
            # Save only the new data
            with open(file_path, 'w') as f:
                json.dump(new_history_data, f, indent=2)
            
            # Print latest dates for comparison
            latest_dune_date = parse_timestamp(latest_date, "%Y-%m-%d", return_datetime=True)
            latest_fetched_date = parse_timestamp(new_items[-1]['timestamp'], timestamp_formats, return_datetime=True) if new_history_data else 'No data'
            print(f"Latest date in Dune: {latest_dune_date}")
            print(f"Latest date in fetched data: {latest_fetched_date}")
            print(f"Saved {len(new_history_data)} new records for subnet {netuid}")
            
        netuids = [subnet['netuid'] for subnet in subnets_to_process]
        validate_subnet_history_files(netuids)

    except Exception as e:
        raise AirflowException(f"Error in fetch_subnet_histories: {e}")

def fetch_dune_baseline_values(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Bittensor subnets stats latest date",
            query_id="4201406"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        
        if df.empty:
            raise AirflowException("No baseline values found in Dune")
            
        print(f"Retrieved baseline values from Dune for date: {df['date'].iloc[0]}")
        print(f"Baseline cumulative_subnet_0_recycled: {df['cumulative_subnet_0_recycled'].iloc[0]}")
        print(f"Baseline other_subnets_recycled: {df['other_subnets_recycled'].iloc[0]}")
        print(f"Baseline total_recycled: {df['total_recycled'].iloc[0]}")
        
        return {
            'baseline_date': df['date'].iloc[0],
            'cumulative_subnet_0_recycled': float(df['cumulative_subnet_0_recycled'].iloc[0]),
            'other_subnets_recycled': float(df['other_subnets_recycled'].iloc[0]),
            'total_recycled': float(df['total_recycled'].iloc[0])
        }
    except Exception as e:
        raise AirflowException(f"Error fetching baseline values: {e}")

def process_subnet_data(**kwargs):
    try:
        print("Starting process_subnet_data")
        print(f"Running in {'DEV' if DEV_MODE else 'PRODUCTION'} mode")
        print(f"Using {'SAVED' if TEST_MODE else 'FRESH'} data")

        # Step 1: Check required directories and files exist
        required_dirs = [DATA_DIR, SUBNET_HISTORY_DIR]
        for dir_path in required_dirs:
            if not os.path.exists(dir_path):
                raise AirflowException(f"Required directory not found: {dir_path}")

        required_files = [
            os.path.join(DATA_DIR, 'subnet_list.json'),
            os.path.join(SUBNET_HISTORY_DIR, 'subnet_0.json')
        ]
        for file_path in required_files:
            if not os.path.exists(file_path):
                raise AirflowException(f"Required file not found: {file_path}")

        # Step 2: Skip validation of latest_dates file in TEST_MODE
        if not TEST_MODE:
            validate_latest_dates_file()
            print("Validated latest dates file")
        else:
            print("Skipping latest dates validation in TEST_MODE")

        # Step 3: Fetch baseline values from Dune
        print("Fetching baseline values from Dune...")
        baseline = fetch_dune_baseline_values()
        baseline_date = datetime.strptime(baseline['baseline_date'], '%Y-%m-%d').date()
        print(f"Using baseline date: {baseline_date}")
        
        # Step 4: Load subnet list and determine which subnets to process
        subnet_list_file = os.path.join(DATA_DIR, 'subnet_list.json')
        with open(subnet_list_file, 'r') as f:
            subnet_data = json.load(f)
            
        subnets_to_process = subnet_data['data'] if not DEV_MODE else subnet_data['data'][:2]

        netuids = [subnet['netuid'] for subnet in subnets_to_process]
        print(f"Processing {len(netuids)} subnets: {netuids}")
        
        # Constants
        ISSUED_VALUE = 7200
        
        # Step 5: Initialize with baseline values
        cumulative_subnet_0_recycled = baseline['cumulative_subnet_0_recycled']
        previous_other_subnets_recycled = baseline['other_subnets_recycled']
        
        print(f"Starting calculations from baseline values:")
        print(f"Initial cumulative_subnet_0_recycled: {cumulative_subnet_0_recycled}")
        print(f"Initial other_subnets_recycled: {previous_other_subnets_recycled}")
        
        # Step 6: Process Subnet 0
        print("Processing Subnet 0...")
        subnet_0_data = {}
        subnet_0_file = os.path.join(SUBNET_HISTORY_DIR, 'subnet_0.json')
        with open(subnet_0_file, 'r') as f:
            subnet_0_history = json.load(f)
            
        for entry in subnet_0_history:
            date = adjust_date(entry['timestamp'])
            entry_date = datetime.strptime(date, '%Y-%m-%d').date()
            if entry_date <= baseline_date:
                continue
            if date not in subnet_0_data:
                subnet_0_data[date] = float(entry['emission']) / 1e9
        
        print(f"Processed {len(subnet_0_data)} days of Subnet 0 data")
        
        # Step 7: Process other subnets
        print("Processing other subnets...")
        other_subnets_recycled_by_date = {}
        
        for netuid in netuids:
            if netuid == 0:
                continue
                
            print(f"Processing subnet {netuid}")
            subnet_file = os.path.join(SUBNET_HISTORY_DIR, f'subnet_{netuid}.json')
            with open(subnet_file, 'r') as f:
                subnet_history = json.load(f)
            
            # Sort by timestamp to ensure chronological order
            subnet_history.sort(key=lambda x: x['timestamp'])
            
            for entry in subnet_history:
                entry_date = datetime.strptime(adjust_date(entry['timestamp']), '%Y-%m-%d').date()
                if entry_date <= baseline_date:
                    continue
                
                date = adjust_date(entry['timestamp'])
                recycled = float(entry['recycled_lifetime']) / 1e9
                
                if date not in other_subnets_recycled_by_date:
                    other_subnets_recycled_by_date[date] = 0
                other_subnets_recycled_by_date[date] += recycled
        
        # Step 8: Combine all data
        all_dates = sorted(set(list(subnet_0_data.keys()) + list(other_subnets_recycled_by_date.keys())))
        print(f"Total unique dates to process: {len(all_dates)}")
        
        # Remove the latest date to avoid premature values
        if all_dates:
            latest_date = all_dates[-1]
            print(f"Removing latest date {latest_date} to avoid premature values")
            all_dates = all_dates[:-1]
        
        # Step 9: Verify continuity
        if all_dates:
            first_new_date = datetime.strptime(all_dates[0], '%Y-%m-%d').date()
            date_gap = (first_new_date - baseline_date).days
            if date_gap > 1:
                raise AirflowException(f"Gap detected between baseline date {baseline_date} and first new date {first_new_date}")
            print(f"Data continuity verified. First new date: {first_new_date}")
        
        # Step 10: Generate final data
        # Step 10: Generate final data
        print("Generating final data...")
        final_data = []
        
        # Initialize with baseline values but don't include them in calculations
        # These are the values as of the baseline date
        cumulative_subnet_0_recycled = baseline['cumulative_subnet_0_recycled']
        previous_other_subnets_recycled = baseline['other_subnets_recycled']
        
        print("Starting values from baseline:")
        print(f"cumulative_subnet_0_recycled: {cumulative_subnet_0_recycled}")
        print(f"previous_other_subnets_recycled: {previous_other_subnets_recycled}")
        
        for date in all_dates:
            subnet_0_emission = subnet_0_data.get(date, 0)
            subnet_0_recycled = ISSUED_VALUE * subnet_0_emission
            
            # Add subnet_0_recycled to the cumulative amount AFTER using it for calculations
            cumulative_subnet_0_recycled += subnet_0_recycled
            
            # Get the current total recycled amount for other subnets
            current_other_subnets_recycled = other_subnets_recycled_by_date.get(date, 0)
            
            # Calculate daily change in other subnets recycled
            daily_other_subnets_recycled = current_other_subnets_recycled - previous_other_subnets_recycled
            
            # Calculate total recycled using CURRENT cumulative values
            total_recycled = cumulative_subnet_0_recycled + current_other_subnets_recycled
            
            # Append data for this date
            final_data.append({
                'date': date,
                'issued': ISSUED_VALUE,
                'daily_issued': ISSUED_VALUE,
                'subnet_0_emission': subnet_0_emission,
                'subnet_0_recycled': subnet_0_recycled,
                'cumulative_subnet_0_recycled': cumulative_subnet_0_recycled,
                'daily_other_subnets_recycled': daily_other_subnets_recycled,
                'other_subnets_recycled': current_other_subnets_recycled,
                'total_recycled': total_recycled
            })
            
            # Update the previous value AFTER using it for calculations
            previous_other_subnets_recycled = current_other_subnets_recycled
            
            if DEV_MODE and len(final_data) <= 5:
                print(f"\nProcessed date: {date}")
                print(f"subnet_0_emission: {subnet_0_emission}")
                print(f"subnet_0_recycled: {subnet_0_recycled}")
                print(f"cumulative_subnet_0_recycled: {cumulative_subnet_0_recycled}")
                print(f"current_other_subnets_recycled: {current_other_subnets_recycled}")
                print(f"daily_other_subnets_recycled: {daily_other_subnets_recycled}")
                print(f"total_recycled: {total_recycled}")
        
        # Convert to DataFrame and save
        df = pd.DataFrame(final_data)
        
        if DEV_MODE:
            print("\nProcessed Data Summary:")
            print("\nFirst few rows:")
            print(df.head())
            print("\nValue ranges:")
            print(df.describe())
            print("\nMin values:")
            print(df.min())
            print("\nMax values:")
            print(df.max())
        
        # Save processed data
        processed_dir = os.path.join(DATA_DIR, 'processed')
        ensure_dir(processed_dir)
        processed_file = os.path.join(processed_dir, 'subnet_summary.csv')
        df.to_csv(processed_file, index=False)
        print(f"Saved processed data to {processed_file}")
        
        return df
        
    except Exception as e:
        print(f"Error in process_subnet_data: {str(e)}")
        raise AirflowException(f"Error in process_subnet_data: {e}")

def upload_to_dune(**kwargs):
    DEV_MODE = False

    try:
        processed_dir = os.path.join(DATA_DIR, 'processed')
        processed_file = os.path.join(processed_dir, 'subnet_summary.csv')
        
        if not os.path.exists(processed_file):
            raise AirflowException(f"Processed file not found: {processed_file}")
        
        df = pd.read_csv(processed_file)
        
        if DEV_MODE:
            print("DEV MODE: Data to be uploaded:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
        else:
            response = insert_df_to_dune(df, 'bittensor_subnets_stats')
            print(f"Uploaded data to Dune. Response: {response}")
            
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")

# Modify the DAG definition section at the bottom of the file
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

dag = DAG(
    'taostats_subnet_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing TaoStats subnet data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
)

get_latest_dates_task = PythonOperator(
    task_id='get_latest_dates',
    python_callable=fetch_dune_table_dates,
    dag=dag,
)

fetch_subnet_list_task = PythonOperator(
    task_id='fetch_subnet_list',
    python_callable=fetch_subnet_list,
    dag=dag,
)

fetch_subnet_histories_task = PythonOperator(
    task_id='fetch_subnet_histories',
    python_callable=fetch_subnet_histories,
    dag=dag,
)

process_subnet_data_task = PythonOperator(
    task_id='process_subnet_data',
    python_callable=process_subnet_data,
    dag=dag,
)

upload_to_dune_task = PythonOperator(
    task_id='upload_to_dune',
    python_callable=upload_to_dune,
    dag=dag,
)

get_latest_dates_task >> fetch_subnet_list_task >> fetch_subnet_histories_task >> process_subnet_data_task >> upload_to_dune_task
