from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os
import time
import shutil

TRIM_MODE = False      # Trim the amount of data fetched and processed
RESET_MODE = False     # Ignore all previous data in Dune and RDS and start from scratch
DO_NOT_UPLOAD = False

# Configuration
# Endpoints for pool history and subnet list
API_ENDPOINTS = {
    'subnet_list': 'https://api.taostats.io/api/subnet/latest/v1',
    'pool_history': "https://api.taostats.io/api/dtao/pool/history/v1"
}
QUERY_LIMIT = 100  # number of records per page

DATA_DIR = '/tmp/bittensor/pool'
TABLE_NAME = 'bittensor_subnet_pools_stats'

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

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
            response = requests.request(method, url, json=data, headers=headers)
            make_api_request.last_request_time = time.time()
            
            print(f"Request URL: {url}")
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
                raise AirflowException(f"Error occurred: {response.status_code, response.text}")
            return None
        except Exception as e:
            print(f"API request failed: {e}")
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

def parse_latest_dates(delta=0):
    """
    Parse both Dune and RDS latest dates and return a dictionary with both dates
    and the minimum of both (to ensure we don't miss any data).
    """
    # Validate and load Dune dates
    latest_dune_dates_file = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
    if not os.path.exists(latest_dune_dates_file):
        raise AirflowException(f"Latest Dune dates file not found: {latest_dune_dates_file}")
    
    latest_dune_dates_df = pd.read_csv(latest_dune_dates_file)
    
    # Validate and load RDS dates
    latest_rds_dates_file = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
    if not os.path.exists(latest_rds_dates_file):
        raise AirflowException(f"Latest RDS dates file not found: {latest_rds_dates_file}")
    
    latest_rds_dates_df = pd.read_csv(latest_rds_dates_file)

    # Extract dates for the specific table
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

def fetch_dune_table_dates(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        from dune_client.client import DuneClient
        from dune_client.query import QueryBase
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Sample Query",
            query_id="4170616" if not RESET_MODE else "4226299"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        print(f"Output from fetch_dune_table_dates: {df.to_dict('records')}")
        
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest dates to {file_path}")
        
    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_table_dates: {e}")

def fetch_rds_table_dates(**kwargs):
    try:
        if not RESET_MODE:
            query = text(f"SELECT '{TABLE_NAME}' as table_name, COALESCE(MAX(date), '2010-01-01') as max_date FROM {TABLE_NAME}")
        else:
            query = text(f"SELECT '{TABLE_NAME}' as table_name, CAST('2010-01-01' as DATE) as max_date FROM {TABLE_NAME}")
        
        df = pd.read_sql(query, rds_engine)
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest RDS dates to {file_path}")
        print(f"Latest RDS dates: {df.to_string()}")
        
    except Exception as e:
        raise AirflowException(f"Error in fetch_rds_table_dates: {e}")

def insert_df_to_dune(df, table_name):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        from dune_client.client import DuneClient
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

def upload_to_dune(**kwargs):
    try:
        processed_file = os.path.join(DATA_DIR, 'processed', 'pool_stats_dune.csv')
        
        if not os.path.exists(processed_file):
            raise AirflowException(f"Processed file not found: {processed_file}")
        
        df = pd.read_csv(processed_file)
        
        if DO_NOT_UPLOAD:
            print("DO NOT UPLOAD: Data to be uploaded to Dune:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
        else:
            response = insert_df_to_dune(df, TABLE_NAME)
            print(f"Uploaded data to Dune. Response: {response}")
            
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")

def upload_to_rds(**kwargs):
    try:
        processed_file = os.path.join(DATA_DIR, 'processed', 'pool_stats_rds.csv')
        
        if not os.path.exists(processed_file):
            raise AirflowException(f"Processed file not found: {processed_file}")
            
        df = pd.read_csv(processed_file)
        
        if DO_NOT_UPLOAD:
            print("DO NOT UPLOAD: Data to be uploaded to RDS:")
            print(df.to_string())
            print(f"Total rows: {len(df)}")
            return

        # Group the DataFrame by date and insert each date's rows in a transaction.
        for date, group_df in df.groupby('date'):
            with rds_engine.begin() as connection:
                group_df.to_sql(
                    TABLE_NAME,
                    connection,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
            print(f"Successfully uploaded data for date {date}")
            
    except Exception as e:
        raise AirflowException(f"Error in upload_to_rds: {e}")

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
    except Exception as e:
        raise AirflowException(f"Error in fetch_subnet_list: {e}")

def fetch_pool_history(**kwargs):
    try:
        # Ensure data directories exist
        ensure_dir(DATA_DIR)
        processed_dir = os.path.join(DATA_DIR, 'processed')
        ensure_dir(processed_dir)
        
        # Determine the cutoff timestamp (only fetch data at least 2 days old)
        cutoff_dt = datetime.utcnow() - timedelta(days=2)
        cutoff_timestamp = int(cutoff_dt.timestamp())
        
        # Parse latest dates from both Dune and RDS
        latest_dates = {}
        if RESET_MODE:
            # If in reset mode, start from epoch
            dune_timestamp = 0
            rds_timestamp = 0
            min_timestamp = 0
        else:
            try:
                # Try to get dates from both sources
                latest_dates = parse_latest_dates()
                
                # Convert to timestamps
                dune_timestamp = int(latest_dates['dune_date'].timestamp())
                rds_timestamp = int(latest_dates['rds_date'].timestamp())
                min_timestamp = int(latest_dates['min_of_max_dates'].timestamp())
                
                print(f"Latest dates - Dune: {latest_dates['dune_date'].date()}, RDS: {latest_dates['rds_date'].date()}")
                print(f"Using minimum date for data fetching: {latest_dates['min_of_max_dates'].date()}")
            except Exception as e:
                print(f"Error parsing latest dates: {e}. Starting from epoch.")
                dune_timestamp = 0
                rds_timestamp = 0
                min_timestamp = 0
        
        # Start with the minimum timestamp to ensure we don't miss any data
        start_timestamp = min_timestamp
        
        # Do not fetch if the latest processed date is already newer than cutoff.
        if start_timestamp >= cutoff_timestamp:
            print("No new data to fetch as the start timestamp is past the cutoff.")
            return
        
        # If TRIM_MODE is enabled, limit the start timestamp to 5 days before cutoff
        if TRIM_MODE:
            trim_start_dt = cutoff_dt - timedelta(days=5)
            trim_start_timestamp = int(trim_start_dt.timestamp())
            if trim_start_timestamp > start_timestamp:
                print(f"TRIM_MODE: Limiting to last 5 days from {trim_start_dt.date()} to {cutoff_dt.date()}")
                start_timestamp = trim_start_timestamp
        
        # Ensure start date is not before 2025-02-14
        min_start_date = datetime(2025, 2, 14)
        min_start_timestamp = int(min_start_date.timestamp())
        if start_timestamp < min_start_timestamp:
            print(f"Setting start date to minimum allowed date: 2025-02-14")
            start_timestamp = min_start_timestamp
        
        # Load the subnet list from file (fetched by fetch_subnet_list task)
        subnet_list_file = os.path.join(DATA_DIR, 'subnet_list.json')
        if not os.path.exists(subnet_list_file):
            raise AirflowException("Subnet list file not found. Please run fetch_subnet_list first.")
        with open(subnet_list_file, 'r') as f:
            subnet_list = json.load(f)
        netuids = [entry['netuid'] for entry in subnet_list['data']]
        if TRIM_MODE:
            netuids = netuids[:3]
        
        all_data = []
        
        # For each netuid, fetch daily records
        for netuid in netuids:
            print(f"Fetching pool history for netuid: {netuid} from {start_timestamp} to {cutoff_timestamp}")
            netuid_data = []
            
            # First, determine the earliest available data point for this netuid
            earliest_timestamp = start_timestamp
            
            # Make a single API call to find the earliest data point
            url = (f"{API_ENDPOINTS['pool_history']}?netuid={netuid}&page=1&limit=1"
                  f"&timestamp_start={start_timestamp}&timestamp_end={cutoff_timestamp}&order=timestamp_asc")
            
            api_key = load_api_key("api_key_taostats")
            response = make_api_request(url, headers={'Authorization': api_key})
            
            if not response or 'data' not in response or not response['data']:
                print(f"No data available for netuid {netuid} in the specified time range. Skipping.")
                continue
                
            # Get the earliest timestamp from the response
            earliest_data = response['data'][0]
            earliest_iso_timestamp = earliest_data['timestamp']
            earliest_dt = pd.to_datetime(earliest_iso_timestamp, format='ISO8601')
            
            # Adjust start date to the day of the earliest data point
            netuid_start_dt = datetime.combine(earliest_dt.date(), datetime.min.time())
            netuid_start_timestamp = int(netuid_start_dt.timestamp())
            
            print(f"Earliest data for netuid {netuid} found at {earliest_dt.date()}")
            
            # Convert timestamps to datetime for date calculations
            start_dt = datetime.fromtimestamp(netuid_start_timestamp)
            cutoff_dt = datetime.fromtimestamp(cutoff_timestamp)
            
            # Create a list of dates between actual start and cutoff
            date_list = []
            current_date = start_dt.date()
            while current_date <= cutoff_dt.date():
                date_list.append(current_date)
                current_date += timedelta(days=1)
            
            print(f"Fetching data for {len(date_list)} days for netuid {netuid}")
            
            # Fetch the first record for each day
            for day_date in date_list:
                # Calculate day start and end timestamps
                day_start = int(datetime.combine(day_date, datetime.min.time()).timestamp())
                day_end = int(datetime.combine(day_date, datetime.max.time()).timestamp())
                
                # Fetch just one record for this day (first record)
                url = (f"{API_ENDPOINTS['pool_history']}?netuid={netuid}&page=1&limit=1"
                      f"&timestamp_start={day_start}&timestamp_end={day_end}&order=timestamp_asc")
                
                api_key = load_api_key("api_key_taostats")
                response = make_api_request(url, headers={'Authorization': api_key})
                
                if not response or 'data' not in response:
                    print(f"No data or invalid response for netuid {netuid} on {day_date}")
                    continue
                    
                data = response['data']
                if not data:
                    print(f"No data for netuid {netuid} on {day_date}")
                    continue
                    
                # Add this day's first record to our dataset
                netuid_data.extend(data)
                print(f"Added record for netuid {netuid} on {day_date}")
            
            # Save raw data for each netuid
            file_path = os.path.join(DATA_DIR, f'pool_history_{netuid}.json')
            with open(file_path, 'w') as f:
                json.dump(netuid_data, f, indent=2)
            print(f"Saved {len(netuid_data)} daily records for netuid {netuid}")
            all_data.extend(netuid_data)
        
        if not all_data:
            print("No pool data fetched.")
            return
        
        # Convert the collected data to a DataFrame
        df_all = pd.DataFrame(all_data)
        
        # Convert the timestamp string to a date (using ISO 8601 format with 'Z' timezone indicator)
        df_all['date'] = pd.to_datetime(df_all['timestamp'], format='ISO8601').dt.date
        
        # Only include records from dates at least 2 days old
        df_all = df_all[df_all['date'] <= cutoff_dt.date()]
        
        # Sort by date and netuid
        df_all = df_all.sort_values(by=['date', 'netuid'])
        
        # Since we've already fetched one record per day per netuid, we don't need to group again
        df_processed = df_all[['date', 'netuid', 'market_cap', 'liquidity', 
                              'total_tao', 'total_alpha', 'alpha_in_pool', 'alpha_staked']]
        
        # Process data for each target (Dune and RDS)
        for target in ['dune', 'rds']:
            print(f"\nProcessing data for {target.upper()}...")
            
            # Get the target-specific start date
            target_timestamp = dune_timestamp if target == 'dune' else rds_timestamp
            target_date = latest_dates.get(f'{target}_date', datetime.fromtimestamp(0)).date()
            
            # Filter data based on the target-specific start date
            target_df = df_processed[df_processed['date'] > target_date]
            
            # Save processed files for specific target
            target_file = os.path.join(processed_dir, f'pool_stats_{target}.csv')
            target_df.to_csv(target_file, index=False)
            print(f"Processed pool stats for {target} saved to {target_file}")
            print(f"Saved {len(target_df)} records for {target}")
            print(f"Date range: {target_df['date'].min()} to {target_df['date'].max()}" if not target_df.empty else "No data to save")
        
    except Exception as e:
        raise AirflowException(f"Error in fetch_pool_history: {e}")

def clear_data_directory(**kwargs):
    """Clears all data from the DATA_DIR before running the DAG."""
    try:
        if os.path.exists(DATA_DIR):
            print(f"Clearing contents of {DATA_DIR}")
            shutil.rmtree(DATA_DIR)
            print(f"Creating fresh {DATA_DIR}")
            os.makedirs(DATA_DIR)
        else:
            print(f"Creating {DATA_DIR}")
            os.makedirs(DATA_DIR)
    except Exception as e:
        raise AirflowException(f"Error clearing data directory: {e}")

# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'sov_taostats_subnet_pools_pipeline',
    default_args=default_args,
    description='A DAG for fetching, processing and uploading pool history data',
    schedule_interval=timedelta(days=3),
    start_date=datetime(2025, 2, 26, 5, 30, 0),
    catchup=False
)

clear_data_directory_task = PythonOperator(
    task_id='clear_data_directory',
    python_callable=clear_data_directory,
    dag=dag,
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

fetch_subnet_list_task = PythonOperator(
    task_id='fetch_subnet_list',
    python_callable=fetch_subnet_list,
    dag=dag,
)

fetch_pool_history_task = PythonOperator(
    task_id='fetch_pool_history',
    python_callable=fetch_pool_history,
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

clear_data_directory_task >> [get_latest_dune_dates_task, get_latest_rds_dates_task, fetch_subnet_list_task] >> fetch_pool_history_task >> [upload_to_dune_task, upload_to_rds_task]
