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
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import os
from airflow.utils.dates import days_ago
import time
from requests.exceptions import RequestException

# Global Development Mode Flag
DEV_MODE = False  # Set to False for production
DO_NOT_UPLOAD = False
RESET_MODE = False

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

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
    'binance-smart-chain': 'binance',
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
DATA_DIR = '/tmp/staking_rewards'
TABLE_NAMES = [blockchain_name + '_staking' for blockchain_name in BLOCKCHAINS.values()]

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
            query_id="4170616" if not RESET_MODE else "4226299"
        )
        results = dune.run_query(query)
        df = pd.DataFrame(results.result.rows)
        print(f"Output from fetch_dune_table_dates: {df.to_dict('records')}")
        
        # Save the latest dates to a file
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_dune_dates.csv')
        df.to_csv(file_path, index=False)
        print(f"Saved latest dates to {file_path}")
        
        validate_latest_dune_dates_file()
    except Exception as e:
        raise AirflowException(f"Error in fetch_dune_table_dates: {e}")

def fetch_rds_table_dates(**kwargs):
    try:
        latest_rds_dates_df = pd.DataFrame()

        for table_name in TABLE_NAMES:
            query = text(f"""
                SELECT '{table_name}' as table_name, COALESCE(MAX(date), '2010-01-01') as max_date 
                FROM {table_name}
            """)
            
            df = pd.read_sql(query, rds_engine)
            latest_rds_dates_df = pd.concat([latest_rds_dates_df, df])
            
        # Save RDS dates to a separate file
        ensure_dir(DATA_DIR)
        file_path = os.path.join(DATA_DIR, 'latest_rds_dates.csv')
        latest_rds_dates_df.to_csv(file_path, index=False)
        print(f"Saved RDS dates to {file_path}")
        print(f"Latest RDS dates: {latest_rds_dates_df.to_string()}")
        
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

    data = {
        'dune_date': [],
        'rds_date': [],
        'min_of_max_dates': []
    }
    table_names = []

    for table_name in TABLE_NAMES:
        try:
            dune_date = latest_dune_dates_df.loc[
                latest_dune_dates_df['table_name'] == table_name,
                'max_date'
            ].iloc[0]

            rds_date = latest_rds_dates_df.loc[
                latest_rds_dates_df['table_name'] == table_name,
                'max_date'
            ].iloc[0]

            print(f'dune_date: {dune_date}')
            print(f'rds_date: {rds_date}')

            # Convert string dates to datetime objects for comparison
            dune_date = datetime.strptime(dune_date, '%Y-%m-%d') + timedelta(days=delta)
            rds_date = datetime.strptime(rds_date, '%Y-%m-%d') + timedelta(days=delta)
            min_of_max_dates = min(dune_date, rds_date)

            print(f"{table_name} - Dune date: {dune_date}, RDS date: {rds_date}, Fetching from date: {min_of_max_dates}")

            # Append values to lists
            table_names.append(table_name)
            data['dune_date'].append(dune_date)
            data['rds_date'].append(rds_date) 
            data['min_of_max_dates'].append(min_of_max_dates)

        except (IndexError, KeyError) as e:
            print(f"Warning: Could not find dates for table {table_name}: {str(e)}")
            continue

    # Create DataFrame with table_names as index
    results = pd.DataFrame(data, index=table_names)
    return results

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
    
    blockchains_to_validate = list(BLOCKCHAINS.items())[:1] if DEV_MODE else BLOCKCHAINS.items()
    
    valid_blockchains = []
    for blockchain_slug, blockchain_name in blockchains_to_validate:
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

# Task functions
def fetch_metadata(**kwargs):
    try:
        validate_latest_dune_dates_file()
        
        api_key = load_api_key("api_key_staking_rewards")
        if not api_key:
            raise ValueError("Staking Rewards API key not found")
        
        latest_dates = parse_latest_dates(0)
        start_dates_df = latest_dates['min_of_max_dates']

        metadata = []
        blockchains_to_process = list(BLOCKCHAINS.items())[:1] if DEV_MODE else BLOCKCHAINS.items()
        metrics_to_process = METRICS

        for blockchain_slug, blockchain_name in blockchains_to_process:
            for metric in metrics_to_process:
                print(f"Processing {blockchain_name} - {metric}")
                
                table_name = f"{blockchain_name}_staking"
                start_date = start_dates_df.loc[table_name] if table_name in start_dates_df.index else None
                end_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                
                days_since_start = (datetime.now() - start_date).days
                
                if days_since_start < 2: # Allow Staking Rewards to catch up
                    print(f"Skipping {blockchain_name} - {metric}: Start date {start_date} is too recent")
                    continue

                start_timestamp = f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z"

                query = f"""
                {{
                  earliest: assets(where: {{slugs: ["{blockchain_slug}"]}}, limit: 1) {{
                    metrics(where: {{metricKeys: ["{metric}"], createdAt_gt: "{start_timestamp}"}}, limit: 1, order: {{createdAt: asc}}) {{
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
                        'start_date': earliest_date or start_timestamp,
                        'end_date': latest_date or end_date,
                    })      
                else:
                    raise AirflowException(f"No data returned for {blockchain_name} - {metric}")
                    

        # Save metadata to file
        ensure_dir(DATA_DIR)
        metadata_file = os.path.join(DATA_DIR, 'metadata.json')

        if DEV_MODE:
            print(f"Metadata: {metadata}")

        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)

        print(f"Saved metadata to {metadata_file}")
        validate_metadata_file()

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

        latest_dates = parse_latest_dates(0)
        dune_dates = latest_dates['dune_date']
        rds_dates = latest_dates['rds_date']

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
                    with open(raw_file, 'r') as f:
                        data = json.load(f)
                    print(f"Loaded {len(data)} data points for {metric} from file {raw_file}")
                    
                    for item in data:
                        date = item['createdAt'].split('T')[0]
                        if date not in csv_data:
                            csv_data[date] = {m: 0 for m in METRICS}
                        csv_data[date][metric] = float(item['defaultValue'])

                # Filter out dates less than 2 days ago
                two_days_ago = datetime.now().date() - timedelta(days=2)
                csv_data = {date: values for date, values in csv_data.items() 
                           if datetime.strptime(date, '%Y-%m-%d').date() <= two_days_ago}
                
                if not csv_data:
                    print(f"No data points found before {two_days_ago} for {blockchain_name}")
                    continue
                
                # Convert to DataFrame
                print(f"Creating DataFrame for {blockchain_name}")
                df = pd.DataFrame.from_dict(csv_data, orient='index')
                df.index.name = 'date'
                df.reset_index(inplace=True)
                
                # Ensure all expected columns are present
                for metric in METRICS:
                    if metric not in df.columns:
                        print(f"Filling missing column with 0: {metric}")
                        df[metric] = 0

                df_dune = df[df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d') > dune_dates.loc[blockchain_name + '_staking'])].copy()
                df_rds = df[df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d') > rds_dates.loc[blockchain_name + '_staking'])].copy()

                df_dune.to_csv(os.path.join(curated_dir, f'{blockchain_name}_staking_curated_dune.csv'), index=False)
                df_rds.to_csv(os.path.join(curated_dir, f'{blockchain_name}_staking_curated_rds.csv'), index=False)

                print(f"Processed {blockchain_name} with {len(df_dune)} rows for Dune and {len(df_rds)} rows for RDS")
                processed_blockchains.append(blockchain_name)
                
            except Exception as e:
                raise AirflowException(f"Error processing {blockchain_name}: {e}")
        
        if not processed_blockchains:
            raise AirflowException("No blockchains were successfully processed")
        
        print(f"Successfully processed blockchains: {processed_blockchains}")
        kwargs['task_instance'].xcom_push(key='processed_blockchains', value=processed_blockchains)
        return processed_blockchains
        
    except Exception as e:
        print(f"Error in process_metrics: {str(e)}")
        raise AirflowException(f"Error in process_metrics: {e}")

def upload_to_dune(**kwargs):
    try:
        # Get the list of successfully processed blockchains from the previous task
        processed_blockchains = kwargs['task_instance'].xcom_pull(task_ids='process_metrics', key='processed_blockchains')
        if not processed_blockchains:
            raise AirflowException("No processed blockchains data available from previous task")
        
        csv_dir = os.path.join(DATA_DIR, 'csv')
        ensure_dir(csv_dir)
        curated_dir = os.path.join(DATA_DIR, 'curated')
        
        uploaded_blockchains = []
        for blockchain_name in processed_blockchains:
            try:
                csv_file = os.path.join(curated_dir, f'{blockchain_name}_staking_curated_dune.csv')
                df = pd.read_csv(csv_file)
                table_name = f"{blockchain_name}_staking"

                if DO_NOT_UPLOAD:
                    print(f"\nDO NOT UPLOAD: Data to be uploaded for {blockchain_name}:")
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
        
        print(f"Successfully uploaded blockchains: {uploaded_blockchains}")
        return uploaded_blockchains
        
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")
    
def upload_to_rds(**kwargs):
    try:
        # Get the list of successfully processed blockchains from the previous task
        processed_blockchains = kwargs['task_instance'].xcom_pull(task_ids='process_metrics', key='processed_blockchains')
        if not processed_blockchains:
            raise AirflowException("No processed blockchains data available from previous task")
        
        curated_dir = os.path.join(DATA_DIR, 'curated')
        ensure_dir(curated_dir)

        for blockchain_name in processed_blockchains:
            csv_file = os.path.join(curated_dir, f"{blockchain_name}_staking_curated_rds.csv")
            
            if not os.path.exists(csv_file):
                raise AirflowException(f"CSV file not found for {blockchain_name}: {csv_file}")
                
            df = pd.read_csv(csv_file)
            table_name = f"{blockchain_name}_staking"
            
            if DO_NOT_UPLOAD:
                print(f"\nDO NOT UPLOAD: Data to be uploaded to RDS for {blockchain_name}:")
                print(df.to_string())
                print(f"Total rows: {len(df)}")
                continue

            df.to_sql(
                table_name,
                rds_engine,
                if_exists='append', 
                index=False,
                method='multi',
                chunksize=1000
            )
            print(f"Successfully uploaded {len(df)} rows to RDS table {table_name}")
            
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
    'sov_staking_rewards_pipeline',
    default_args=default_args,
    description='A DAG for fetching and processing Staking Rewards data',
    schedule_interval=timedelta(days=3),
    start_date=datetime(2025, 2, 26, 3, 30, 0),
    catchup=False
)

fetch_dune_table_dates_task = PythonOperator(
    task_id='get_latest_dune_dates',
    python_callable=fetch_dune_table_dates,
    dag=dag,
)

fetch_rds_table_dates_task = PythonOperator(
    task_id='get_latest_rds_dates',
    python_callable=fetch_rds_table_dates,
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

process_metrics_task = PythonOperator(
    task_id='process_metrics',
    python_callable=process_metrics,
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
[fetch_dune_table_dates_task, fetch_rds_table_dates_task] >> fetch_metadata_task >> fetch_metrics_task >> process_metrics_task >> [upload_to_dune_task, upload_to_rds_task]

