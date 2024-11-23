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
API_ENDPOINT = 'https://api.stakingrewards.com/public/query'
QUERY_LIMIT = 500
BLOCKCHAINS = {
    'ethereum-2-0': 'ethereum',
    'bittensor': 'bittensor', # Bittensor is using Taostats for staking amounts but we need it for circulating supply
    'solana': 'solana',
    'near-protocol': 'near',
    'aptos': 'aptos',
    'avalanche': 'avalanche',
    'the-open-network': 'ton',
    'celestia': 'celestia',
    'sui': 'sui',
    # 'binance-smart-chain': 'binance',
    'sei-network': 'sei'
}
METRICS = [
    'reward_rate',
    'real_reward_rate',
    'inflation_rate',
    # 'price', -- using dune prices
    'staked_tokens',
    'staking_ratio',
    'marketcap',
    'circulating_supply',
    'circulating_percentage'
]

# Data directory
DATA_DIR = '/opt/airflow/data/staking_rewards'

# Utility functions
def load_api_key(key_name):
    try:
        return Variable.get(key_name)
    except KeyError:
        print(f"API key '{key_name}' not found. Please set it in Airflow Variables.")
        return None

def make_api_request(url, method='POST', headers=None, data=None, max_retries=3, retry_delay=60):
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
        
        # Save the latest dates to a file
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

# Input validation functions
def validate_latest_dates_file():
    latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
    if not os.path.exists(latest_dates_file):
        raise AirflowException(f"Latest dates file not found: {latest_dates_file}")
    df = pd.read_csv(latest_dates_file)
    if not all(col in df.columns for col in ['table_name', 'max_date']):
        raise AirflowException(f"Invalid format in latest_dates.csv. Expected columns: table_name, max_date")

def validate_metadata_file():
    metadata_file = os.path.join(DATA_DIR, 'metadata.json')
    if not os.path.exists(metadata_file):
        raise AirflowException(f"Metadata file not found: {metadata_file}")
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    if not isinstance(metadata, list) or not all(isinstance(item, dict) for item in metadata):
        raise AirflowException("Invalid metadata format. Expected a list of dictionaries.")
    required_keys = ['chain', 'slug', 'metric', 'start_date', 'end_date']
    if not all(all(key in item for key in required_keys) for item in metadata):
        raise AirflowException(f"Invalid metadata format. Each item should contain keys: {required_keys}")

def validate_raw_files():
    raw_dir = os.path.join(DATA_DIR, 'raw')
    if not os.path.exists(raw_dir):
        raise AirflowException(f"Raw data directory not found: {raw_dir}")
    
    valid_blockchains = []
    for blockchain_slug, blockchain_name in BLOCKCHAINS.items():
        is_valid = True
        for metric in METRICS:
            raw_file = os.path.join(raw_dir, f'{blockchain_slug}_{metric}_raw.json')
            if not os.path.exists(raw_file):
                print(f"Raw file not found for {blockchain_name}: {raw_file}")
                is_valid = False
                break
            try:
                with open(raw_file, 'r') as f:
                    data = json.load(f)
                if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                    print(f"Invalid format in {raw_file} for {blockchain_name}")
                    is_valid = False
                    break
            except Exception as e:
                print(f"Error reading raw file for {blockchain_name}: {str(e)}")
                is_valid = False
                break
        
        if is_valid:
            valid_blockchains.append(blockchain_name)
    
    return valid_blockchains

def validate_curated_files(blockchains_to_validate):
    curated_dir = os.path.join(DATA_DIR, 'curated')
    if not os.path.exists(curated_dir):
        raise AirflowException(f"Curated data directory not found: {curated_dir}")
    
    valid_blockchains = []
    columns_to_check = ['date'] + METRICS
    
    for blockchain_name in blockchains_to_validate:
        try:
            curated_file = os.path.join(curated_dir, f'{blockchain_name}_staking_curated.json')
            if not os.path.exists(curated_file):
                print(f"Curated file not found: {curated_file}")
                continue
                
            df = pd.read_json(curated_file)
            
            if not all(col in df.columns for col in columns_to_check):
                print(f"Invalid format in {curated_file}. Expected columns: {', '.join(columns_to_check)}. Got columns: {', '.join(df.columns)}")
                continue
            
            valid_blockchains.append(blockchain_name)
            print(f"Validated curated file for {blockchain_name}")
            
        except Exception as e:
            print(f"Error validating curated file for {blockchain_name}: {str(e)}")
            continue
    
    return valid_blockchains

# Task functions
def fetch_metadata(**kwargs):
    try:
        validate_latest_dates_file()
        
        api_key = load_api_key("api_key_staking_rewards")
        if not api_key:
            raise ValueError("Staking Rewards API key not found")
        
        latest_dates_file = os.path.join(DATA_DIR, 'latest_dates.csv')
        latest_dates_df = pd.read_csv(latest_dates_file)
        print(f"Read latest dates from {latest_dates_file}")
        print(f"Latest dates DataFrame: {latest_dates_df.to_dict('records')}")
        
        latest_dates_df = latest_dates_df.set_index('table_name')

        metadata = []
        # Use global DEV_MODE
        blockchains_to_process = list(BLOCKCHAINS.items())[:1] if DEV_MODE else BLOCKCHAINS.items()
        metrics_to_process = METRICS[:1] if DEV_MODE else METRICS

        for blockchain_slug, blockchain_name in blockchains_to_process:
            for metric in metrics_to_process:
                print(f"Processing {blockchain_name} - {metric}")
                
                table_name = f"{blockchain_name}_staking"
                
                start_date = latest_dates_df.loc[table_name, 'max_date'] if table_name in latest_dates_df.index else None
                
                print(f"Start date: {start_date} for {blockchain_name} - {metric}")
                start_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
                start_date = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')

                end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Yesterday's date

                query = f"""
                {{
                  earliest: assets(where: {{slugs: ["{blockchain_slug}"]}}, limit: 1) {{
                    metrics(where: {{metricKeys: ["{metric}"], createdAt_gt: "{start_date}"}}, limit: 1, order: {{createdAt: asc}}) {{
                      createdAt
                    }}
                  }}
                  latest: assets(where: {{slugs: ["{blockchain_slug}"]}}, limit: 1) {{
                    metrics(where: {{metricKeys: ["{metric}"]}}, limit: 1, order: {{createdAt: desc}}) {{
                      createdAt
                    }}
                  }}
                }}
                """

                print(f"Making Staking Rewards API request: {query}")
                response = make_api_request(API_ENDPOINT, headers={'X-API-KEY': api_key}, data={'query': query})
                
                if response is None:
                    raise AirflowException(f"API request failed: No response received from the Staking Rewards API for {blockchain_name} - {metric}.")
                
                print(f"Response: {response}")
                if response and response.get('data'):
                    print(f"Response data: {response['data']}")
                    earliest = response['data'].get('earliest', [])
                    latest = response['data'].get('latest', [])
                    print(f"Earliest: {earliest}")
                    print(f"Latest: {latest}")
                    
                    earliest_date = None
                    latest_date = None
                    
                    if earliest and earliest[0].get('metrics'):
                        earliest_date = earliest[0]['metrics'][0].get('createdAt')
                    if latest and latest[0].get('metrics'):
                        latest_date = latest[0]['metrics'][0].get('createdAt')
                    
                    print(f"Earliest date: {earliest_date}")
                    print(f"Latest date: {latest_date}")
                    
                    metadata.append({
                        'chain': blockchain_name,
                        'slug': blockchain_slug,
                        'metric': metric,
                        'start_date': earliest_date or start_date,
                        'end_date': latest_date or end_date,
                    })      
                else:
                    raise AirflowException(f"No data returned for {blockchain_name} - {metric}")
                    

        # Save metadata to file
        ensure_dir(DATA_DIR)
        metadata_file = os.path.join(DATA_DIR, 'metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)
        print(f"Saved metadata to {metadata_file}")
        
        validate_metadata_file()
    except AirflowException as e:
        print(f"AirflowException: {e}")
        raise
    except Exception as e:
        raise AirflowException(f"Error in fetch_metadata: {e}")
    
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

def fetch_metrics(**kwargs):
    try:
        validate_metadata_file()
        
        api_key = load_api_key("api_key_staking_rewards")
        if not api_key:
            raise AirflowException("Staking Rewards API key not found")
        
        metadata_file = os.path.join(DATA_DIR, 'metadata.json')
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        print(f"Read metadata from {metadata_file}")

        # Use global DEV_MODE
        if DEV_MODE:
            metadata = metadata[:1]
            print("DEV MODE: Processing only the first chain-metric combination")

        print(f"Metadata: {metadata}")

        for entry in metadata:
            blockchain_slug = entry['slug']
            metric = entry['metric']
            start_date = entry['start_date']
            end_date = entry['end_date']

            print(f"Processing: {blockchain_slug} - {metric}")

            metric_data = []
            current_start = start_date

            while current_start <= end_date:
                # Parse dates and adjust them
                parsed_start = datetime.strptime(current_start.split('.')[0].rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
                parsed_end = datetime.strptime(end_date.split('.')[0].rstrip('Z'), '%Y-%m-%dT%H:%M:%S')
                
                query_start = (parsed_start - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
                query_end = (parsed_end + timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

                query = f"""
                {{
                  assets(where: {{slugs: ["{blockchain_slug}"]}}, limit: 1) {{
                    metrics(where: {{metricKeys: ["{metric}"], createdAt_gt: "{query_start}", createdAt_lt: "{query_end}"}}, limit: {QUERY_LIMIT}, interval: day, order: {{createdAt: asc}}) {{
                      defaultValue
                      createdAt
                    }}
                  }}
                }}
                """

                response = make_api_request(API_ENDPOINT, method='POST', headers={'X-API-KEY': api_key}, data={'query': query})

                print(f"query: {query}")
                print(f"response: {response}")

                if response is None:
                    raise AirflowException("API request failed: No response received from the Staking Rewards API.")
                
                if response and response.get('data', {}).get('assets') and response['data']['assets'][0].get('metrics'):
                    metrics = response['data']['assets'][0]['metrics']
                    metric_data.extend(metrics)
                    if len(metrics) < QUERY_LIMIT:
                        break
                    current_start = metrics[-1]['createdAt']
                else:
                    break

            # Save raw data to file
            raw_dir = os.path.join(DATA_DIR, 'raw')
            ensure_dir(raw_dir)
            raw_file = os.path.join(raw_dir, f'{blockchain_slug}_{metric}_raw.json')
            with open(raw_file, 'w') as f:
                json.dump(metric_data, f)
            print(f"Saved raw data to {raw_file}")

            if DEV_MODE:
                print("DEV MODE: Exiting after processing one chain-metric combination")
                break
            
    except Exception as e:
        raise AirflowException(f"Error in fetch_metrics: {e}")

def process_metrics(**kwargs):
    try:
        print("Starting process_metrics function")
        valid_blockchains = validate_raw_files()
        if not valid_blockchains:
            raise AirflowException("No valid raw files found for any blockchain")
        
        print(f"Processing valid blockchains: {valid_blockchains}")
        processed_blockchains = []
        
        # Read raw data and convert to CSV format
        raw_dir = os.path.join(DATA_DIR, 'raw')
        curated_dir = os.path.join(DATA_DIR, 'curated')
        ensure_dir(curated_dir)
        
        for blockchain_name in valid_blockchains:
            try:
                print(f"Processing blockchain: {blockchain_name}")
                blockchain_slug = next(slug for slug, name in BLOCKCHAINS.items() if name == blockchain_name)
                
                csv_data = {}
                for metric in METRICS:
                    raw_file = os.path.join(raw_dir, f'{blockchain_slug}_{metric}_raw.json')
                    print(f"  Checking raw file: {raw_file}")
                    with open(raw_file, 'r') as f:
                        data = json.load(f)
                    print(f"    Loaded {len(data)} data points for {metric}")
                    
                    for item in data:
                        date = item['createdAt'].split('T')[0]
                        if date not in csv_data:
                            csv_data[date] = {m: 0 for m in METRICS}
                        csv_data[date][metric] = float(item['defaultValue'])
                
                # Convert to DataFrame
                print(f"Creating DataFrame for {blockchain_name}")
                df = pd.DataFrame.from_dict(csv_data, orient='index')
                df.index.name = 'date'
                df.reset_index(inplace=True)
                
                # Ensure all expected columns are present
                for metric in METRICS:
                    if metric not in df.columns:
                        print(f"  Adding missing column: {metric}")
                        df[metric] = 0
                
                # Save curated file
                curated_file = os.path.join(curated_dir, f'{blockchain_name}_staking_curated.json')
                df.to_json(curated_file, orient='records')
                print(f"Saved curated data to {curated_file}")
                
                processed_blockchains.append(blockchain_name)
                
            except Exception as e:
                print(f"Error processing {blockchain_name}: {str(e)}")
                continue
        
        if not processed_blockchains:
            raise AirflowException("No blockchains were successfully processed")
        
        # Validate only the processed blockchains
        valid_curated = validate_curated_files(processed_blockchains)
        if not valid_curated:
            raise AirflowException("No valid curated files were created")
        
        print(f"Successfully processed blockchains: {valid_curated}")
        kwargs['task_instance'].xcom_push(key='processed_blockchains', value=valid_curated)
        return valid_curated
        
    except Exception as e:
        print(f"Error in process_metrics: {str(e)}")
        raise AirflowException(f"Error in process_metrics: {e}")

def upload_to_dune(**kwargs):
    DEV_MODE = False

    try:
        # Get the list of successfully processed blockchains from the previous task
        processed_blockchains = kwargs['task_instance'].xcom_pull(task_ids='convert_to_csv', key='processed_blockchains')
        if not processed_blockchains:
            raise AirflowException("No processed blockchains data available from previous task")
        
        # Validate only the processed blockchains
        valid_blockchains = validate_curated_files(processed_blockchains)
        if not valid_blockchains:
            raise AirflowException("No valid curated files found for upload")
        
        csv_dir = os.path.join(DATA_DIR, 'csv')
        ensure_dir(csv_dir)
        curated_dir = os.path.join(DATA_DIR, 'curated')
        
        uploaded_blockchains = []
        for blockchain_name in valid_blockchains:
            try:
                curated_file = os.path.join(curated_dir, f'{blockchain_name}_staking_curated.json')
                df = pd.read_json(curated_file)
                table_name = f"{blockchain_name}_staking"
                
                # Save final CSV to file
                csv_path = os.path.join(csv_dir, f'{table_name}.csv')
                df.to_csv(csv_path, index=False)
                print(f"Saved CSV to {csv_path}")

                if DEV_MODE:
                    print(f"\nDEV MODE: Data to be uploaded for {blockchain_name}:")
                    print(df.to_string())
                    print(f"Total rows: {len(df)}")
                    continue
                
                # Upload to Dune
                response = insert_df_to_dune(df, table_name)
                print(f"Uploaded {table_name} to Dune. Response: {response}")
                uploaded_blockchains.append(blockchain_name)
                
            except Exception as e:
                print(f"Failed to upload {blockchain_name}: {str(e)}")
                continue
        
        if not uploaded_blockchains:
            raise AirflowException("No blockchains were successfully uploaded to Dune")
        
        print(f"Successfully uploaded blockchains: {uploaded_blockchains}")
        return uploaded_blockchains
        
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
    'staking_rewards_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing Staking Rewards data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
)

get_latest_dates_task = PythonOperator(
    task_id='get_latest_dates',
    python_callable=fetch_dune_table_dates,
    dag=dag,
)

fetch_metadata_task = PythonOperator(
    task_id='fetch_metadata',
    python_callable=fetch_metadata,
    dag=dag,
)

fetch_metrics_task = PythonOperator(
    task_id='fetch_metrics',
    python_callable=fetch_metrics,
    dag=dag,
)

convert_to_csv_task = PythonOperator(
    task_id='convert_to_csv',
    python_callable=process_metrics,
    dag=dag,
)

upload_to_dune_task = PythonOperator(
    task_id='upload_to_dune',
    python_callable=upload_to_dune,
    dag=dag,
)

# Set up task dependencies
get_latest_dates_task >> fetch_metadata_task >> fetch_metrics_task >> convert_to_csv_task >> upload_to_dune_task

