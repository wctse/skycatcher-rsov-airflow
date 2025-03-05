import os
import re
import time
import glob
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# ---------------- Global Flags ---------------- #
RESET_MODE = False
DO_NOT_UPLOAD = False

# ---------------- Logging Setup ---------------- #
logger = logging.getLogger("defillama_apys")
logger.setLevel(logging.INFO)

# ---------------- Utility Functions ---------------- #
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")
    else:
        logger.info(f"Directory exists: {directory}")

def load_api_key(key_name):
    """
    Loads API key from Airflow Variables.
    """
    try:
        key = Variable.get(key_name)
        logger.info(f"Loaded API key for {key_name}.")
        return key
    except Exception as e:
        raise AirflowException(f"API key '{key_name}' not found: {e}")

def make_api_request(url, method='GET', headers=None, data=None, max_retries=3, retry_delay=60):
    """
    Reusable API request with retry logic.
    """
    headers = headers or {}
    headers.setdefault('Content-Type', 'application/json')
    last_request_time = getattr(make_api_request, 'last_request_time', 0)
    
    for attempt in range(max_retries):
        current_time = time.time()
        if current_time - last_request_time < 0.1:
            sleep_time = 0.1 - (current_time - last_request_time)
            logger.info(f"Sleeping {sleep_time:.2f} seconds to respect rate limits.")
            time.sleep(sleep_time)
        try:
            response = requests.request(method, url, json=data, headers=headers)
            make_api_request.last_request_time = time.time()
            logger.info(f"Request URL: {url} | Status: {response.status_code}")
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning(f"Rate limited on attempt {attempt+1} for URL: {url}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise AirflowException("Max retries reached due to rate limiting.")
            else:
                raise AirflowException(f"Unexpected response {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Error during API request: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise AirflowException(f"Max retries reached with error: {e}")
    return None

def validate_csv_file(file_path, required_columns):
    """
    Validates that the CSV file exists and contains the required columns.
    """
    if not os.path.exists(file_path):
        raise AirflowException(f"File not found: {file_path}")
    try:
        df = pd.read_csv(file_path, nrows=5)
    except Exception as e:
        raise AirflowException(f"Error reading {file_path}: {e}")
    for col in required_columns:
        if col not in df.columns:
            raise AirflowException(f"Missing required column '{col}' in {file_path}")
    return True

# ---------------- Global Config ---------------- #
# Base directories (using temporary storage for incremental processing)
BASE_DATA_DIR = '/tmp/defillama'
RAW_DATA_DIR = os.path.join(BASE_DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DATA_DIR, 'processed')
METADATA_DIR = os.path.join(BASE_DATA_DIR, 'metadata')

for d in [BASE_DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, METADATA_DIR]:
    ensure_dir(d)

# Create RDS engine from Airflow connection (ensure 'rds_connection' is set in Airflow)
rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

# ---------------- Task Functions ---------------- #
def fetch_latest_rds_date(**kwargs):
    """
    Fetch the current maximum date from the 'defi_pool_apys' table.
    If RESET_MODE is enabled, returns '2010-01-01' to force reprocessing.
    Returns a string in YYYY-MM-DD format.
    """
    try:
        if RESET_MODE:
            logger.info("RESET_MODE enabled. Using reset date '2010-01-01'.")
            return '2010-01-01'
        query = text("SELECT MAX(date) as max_date FROM defi_pool_apys")
        df = pd.read_sql(query, rds_engine)
        if df.empty or pd.isnull(df.loc[0, 'max_date']):
            max_date = '2010-01-01'
        else:
            max_date = pd.to_datetime(df.loc[0, 'max_date']).strftime('%Y-%m-%d')
        logger.info(f"Current max date in DB: {max_date}")
        return max_date
    except Exception as e:
        raise AirflowException(f"Error fetching max date: {e}")

def fetch_metadata(**kwargs):
    """
    Fetches pools and protocols metadata from the DefiLlama API and saves them as JSON files.
    Endpoints:
      - Pools: https://pro-api.llama.fi/APIKEY/yields/poolsOld
      - Protocols: https://pro-api.llama.fi/APIKEY/api/protocols
    """
    try:
        api_key = load_api_key("api_key_defillama")
        base_url = "https://pro-api.llama.fi"
        pools_url = f"{base_url}/{api_key}/yields/poolsOld"
        protocols_url = f"{base_url}/{api_key}/api/protocols"
        
        pools_response = make_api_request(pools_url)
        protocols_response = make_api_request(protocols_url)
        
        pools_file = os.path.join(METADATA_DIR, 'raw-pools.json')
        protocols_file = os.path.join(METADATA_DIR, 'raw-protocols.json')
        with open(pools_file, 'w') as f:
            json.dump(pools_response, f, indent=2)
        with open(protocols_file, 'w') as f:
            json.dump(protocols_response, f, indent=2)
        logger.info(f"Saved metadata to {pools_file} and {protocols_file}")
    except Exception as e:
        raise AirflowException(f"Error in fetch_metadata: {e}")

def fetch_pool_yield_data(**kwargs):
    """
    Loads metadata for pools and protocols from the stored JSON files,
    filters pools, and fetches yield data from DefiLlama using make_api_request.
    Saves each pool's raw data as a CSV in RAW_DATA_DIR and stores processed pools metadata for later use.
    """
    try:
        # Load metadata JSON files from METADATA_DIR
        pools_metadata_file = os.path.join(METADATA_DIR, 'raw-pools.json')
        protocols_metadata_file = os.path.join(METADATA_DIR, 'raw-protocols.json')
        with open(pools_metadata_file, 'r', encoding='utf-8') as f:
            raw_pools = json.load(f)
        with open(protocols_metadata_file, 'r', encoding='utf-8') as f:
            raw_protocols = json.load(f)
        logger.info("Loaded pools and protocols metadata from stored JSON files.")

        # Define ETH tokens
        eths = pd.DataFrame([
            ('ETH', '0x0000000000000000000000000000000000000000'),
            ('WETH', '0x4200000000000000000000000000000000000006'),
            ('stETH', '0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'),
            ('wstETH', '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
            ('rETH', '0xae78736Cd615f374D3085123A210448E74Fc6393'),
            ('wBETH', '0xa2e3356610840701bdf5611a53974510ae27e2e1'),
            ('mETH', '0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa'),
            ('frxETH', '0x5e8422345238f34275888049021821e8e08caa1f'),
            ('sfrxETH', '0xac3E018457B222d93114458476f3E3416Abbe38F'),
            ('cbETH', '0xbe9895146f7af43049ca1c1ae358b0541ea49704'),
            ('osETH', '0xf1c9acdc66974dfb6decb12aa385b9cd01190e38'),
            ('ankrETH', '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb'),
            ('LsETH', '0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549'),
            ('OETH', '0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3'),
            ('swETH', '0xf951e335afb289353dc249e82926178eac7ded78'),
            ('rswETH', '0xfae103dc9cf190ed75350761e95403b7b8afa6c0'),
            ('ezETH', '0xbf5495efe5db9ce00f80364c8b423567e58d2110'),
            ('WEETH', '0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee'),
            ('rsETH', '0xa1290d69c65a6fe4df752f95823fae25cb99e5a7'),
            ('pufETH', '0xd9a442856c234a39a81a089c06451ebaa4306a72')
        ], columns=['symbol', 'address'])
        eth_addresses = set(eths['address'].str.lower())
        eth_lookup = dict(zip(eths['address'].str.lower(), eths['symbol']))

        # Filter by supported chains
        chains = [
            'Ethereum', 'Base', 'Arbitrum', 'Polygon', 'Optimism', 'Scroll',
            'Starknet', 'Mode', 'Linea', 'zkSync Era', 'Fraxtal', 'Manta',
            'Metis', 'Blast', 'Mantle', 'Polygon zkEVM', 'Zksync'
        ]

        pools = pd.DataFrame(raw_pools['data'])
        pools = pools[['symbol', 'chain', 'pool', 'underlyingTokens', 'project', 'pool_old']]
        protocols = pd.DataFrame(raw_protocols)
        protocols = protocols[['slug', 'category']]
        pools = pools.merge(protocols, left_on='project', right_on='slug', how='left')
        pools = pools[pools['chain'].isin(chains)]
        pools = pools[pools['underlyingTokens'].notna()]
        pools = pools[~pools['underlyingTokens'].apply(lambda x: all(token is None for token in x))]
        pools['has_eth'] = pools['underlyingTokens'].apply(lambda x: any(addr.lower() in eth_addresses for addr in x))
        pools = pools[pools['has_eth']]
        pools['underlyingTokens'] = pools['underlyingTokens'].apply(
            lambda token_list: [eth_lookup.get(addr.lower(), addr) for addr in token_list]
        )
        pools['pool_old'] = pools['pool_old'].str.replace(r'^.*?(?=0x)', '', regex=True).str.slice(0, 40)
        pools['pool_old'] = pools['pool_old'].apply(lambda x: x if x.startswith('0x') else '')
        pools.rename(columns={'pool_old': 'address'}, inplace=True)
        pools['name'] = pools['project'] + ' ' + pools['symbol'] + ' ' + pools['address']
        pools = pools[['name', 'pool', 'chain', 'address', 'underlyingTokens', 'category']]
        pools = pools.drop_duplicates(subset=['name'], keep='first').reset_index(drop=True)
        logger.info(f"Filtered to {len(pools)} pools after applying chain and token filters.")

        # Save processed pools metadata for later merging
        pools.to_csv(os.path.join(PROCESSED_DATA_DIR, 'pools.csv'), index=False)

        # Load DefiLlama API key from Airflow Variables
        api_key = load_api_key("api_key_defillama")
        base_url = "https://pro-api.llama.fi"
        
        # For each pool, fetch yield data using the reusable API function.
        for _, row in pools.iterrows():
            pool_name = row['name']
            safe_name = re.sub(r'\s+', '_', pool_name)
            out_file = os.path.join(RAW_DATA_DIR, f"{safe_name}.csv")
            url = f"{base_url}/{api_key}/yields/chart/{row['pool']}"
            try:
                result = make_api_request(url)
                if result and 'data' in result:
                    df = pd.DataFrame(result['data'])
                    if not df.empty:
                        df.to_csv(out_file, index=False)
                        logger.info(f"Saved raw data for pool {pool_name} to {out_file}")
                else:
                    logger.warning(f"No data returned for pool {pool_name}.")
            except Exception as e:
                logger.error(f"Error fetching data for {pool_name}: {e}")
            time.sleep(0.1)  # slight delay between API calls
    except Exception as e:
        raise AirflowException(f"Error in fetch_pool_yield_data: {e}")

def process_apys_data(**kwargs):
    try:
        # Get current max date from RDS (via XCom)
        ti = kwargs['ti']
        current_max_date_str = ti.xcom_pull(task_ids='get_latest_rds_date')
        current_max_date = pd.to_datetime(current_max_date_str)
        logger.info(f"Processing new data: only rows with date > {current_max_date_str}")

        # Load pools metadata (processed in fetch_pool_yield_data)
        pools_file = os.path.join(PROCESSED_DATA_DIR, 'pools.csv')
        pools = pd.read_csv(pools_file)

        # --- Read all raw CSV files ---
        csv_files = glob.glob(os.path.join(RAW_DATA_DIR, '*.csv'))
        if not csv_files:
            logger.info("No raw CSV files found in RAW_DATA_DIR")
            return

        # Load each CSV into a dict (keyed by file name without extension)
        dfs = {}
        for file in csv_files:
            base_name = os.path.basename(file)
            pool_key = os.path.splitext(base_name)[0]  # full pool name with underscores
            try:
                df = pd.read_csv(file)
                dfs[pool_key] = df
            except Exception as e:
                logger.warning(f"Skipping {file} due to error: {e}")

        # --- Calculate date ranges & filter out pools with >25% missing dates ---
        date_ranges = {}
        for key, df in dfs.items():
            if 'timestamp' not in df.columns:
                continue
            # Convert timestamp column to dates
            df['date'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')
            # Skip if date conversion failed entirely
            if df['date'].isnull().all():
                continue
            dates = pd.to_datetime(df['date'])
            min_date = dates.min()
            max_date = dates.max()
            expected_days = (max_date - min_date).days + 1
            actual_days = df['date'].nunique()
            missing_days = expected_days - actual_days
            missing_ratio = missing_days / expected_days if expected_days > 0 else 1.0
            date_ranges[key] = {
                'min_date': min_date,
                'max_date': max_date,
                'expected_days': expected_days,
                'actual_days': actual_days,
                'missing_days': missing_days,
                'missing_ratio': missing_ratio
            }
        valid_pool_keys = [key for key, stats in date_ranges.items() if stats['missing_ratio'] <= 0.25]
        logger.info(f"Pools passing date completeness check: {valid_pool_keys}")

        # --- Process valid pools and filter out ones with zero or missing APY ---
        valid_dfs = []
        removed_zero_apy = []
        for key in valid_pool_keys:
            df = dfs[key]
            # Convert timestamp to date (as string) and to datetime for filtering
            df['date'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')
            df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
            # Only keep rows that are newer than current_max_date
            df = df[df['date_dt'] > current_max_date]
            if df.empty:
                continue
            # Check for valid APY values
            if 'apy' not in df.columns or df['apy'].isna().all() or (df['apy'] == 0).all():
                removed_zero_apy.append(key)
                continue
            # Preserve the full pool name (from file name, converting underscores to spaces)
            df['full_pool_name'] = key.replace('_', ' ')
            # Remove the most recent 2 days from current date
            current_date = pd.Timestamp.now().normalize()
            cutoff_date = current_date - pd.Timedelta(days=2)
            df = df[df['date_dt'] <= cutoff_date]
            if df.empty:
                continue
            valid_dfs.append(df)

        if removed_zero_apy:
            logger.info(f"Pools removed due to zero/missing APY: {removed_zero_apy}")
        if not valid_dfs:
            logger.info("No valid new data found after filtering.")
            return

        # --- Combine valid dataframes ---
        apys = pd.concat(valid_dfs, ignore_index=True)

        # Merge with pools metadata on full_pool_name (which was originally created as "project symbol address")
        apys = apys.merge(pools[['name', 'category']], left_on='full_pool_name', right_on='name', how='left')

        # Ensure the date is in the proper format
        apys['date'] = pd.to_datetime(apys['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Extract contract address from full_pool_name (assuming it is the third token)
        apys['contract'] = apys['full_pool_name'].apply(lambda x: x.split()[2] if len(x.split()) >= 3 else '')
        # Create a display pool name (using the first two tokens)
        apys['pool_name'] = apys['full_pool_name'].apply(lambda x: ' '.join(x.split()[:2]))

        apys.rename(columns={'tvlUsd': 'tvl_usd'}, inplace=True)
        # Select final columns
        final_cols = ['date', 'pool_name', 'contract', 'category', 'apy', 'tvl_usd']
        final_df = apys[final_cols]

        # Save the incremental CSV (for upload to RDS)
        incremental_file = os.path.join(PROCESSED_DATA_DIR, 'pool_apys_incremental.csv')
        final_df.to_csv(incremental_file, index=False)
        logger.info(f"Saved incremental data file with {len(final_df)} rows.")

        # --- Calculate weighted average APY (using original logic) ---
        # Group by date and category, then compute the weighted average using tvlUsd as weights.
        weighted_avg_apy = (
            final_df.groupby(['date', 'category'])
            .apply(lambda x: np.average(x['apy'], weights=x['tvl_usd']))
            .reset_index()
            .rename(columns={0: 'weighted_avg_apy'})
        )

        # Pivot so that each category becomes a column
        weighted_avg_apy_pivot = weighted_avg_apy.pivot(index='date', columns='category', values='weighted_avg_apy')
        weighted_avg_apy_pivot.sort_index(inplace=True)

        # Save the pivot table
        weighted_avg_file = os.path.join(PROCESSED_DATA_DIR, 'weighted_avg_apy_pivot.csv')
        weighted_avg_apy_pivot.to_csv(weighted_avg_file)
        logger.info("Saved weighted average APY pivot table.")

    except Exception as e:
        raise AirflowException(f"Error in process_apys_data: {e}")


def upload_to_rds(**kwargs):
    """
    Reads the incremental processed CSV file and uploads its rows to the 'defi_pool_apys' table.
    If DO_NOT_UPLOAD is enabled, logs the data instead of uploading.
    """
    try:
        incremental_file = os.path.join(PROCESSED_DATA_DIR, 'pool_apys_incremental.csv')
        if not os.path.exists(incremental_file):
            logger.info("No incremental file to upload. Skipping upload.")
            return
        df = pd.read_csv(incremental_file)
        if df.empty:
            logger.info("Incremental file is empty. Nothing to upload.")
            return
        if DO_NOT_UPLOAD:
            logger.info("DO_NOT_UPLOAD enabled. Data to be uploaded:")
            logger.info(df.head(10).to_string())
            logger.info(f"Total rows: {len(df)}")
        else:
            df.to_sql(
                'defi_pool_apys',
                rds_engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Uploaded {len(df)} new rows to defi_pool_apys.")
    except Exception as e:
        raise AirflowException(f"Error uploading data to RDS: {e}")

# ---------------- DAG Definition ---------------- #
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'apys_defillama_pipeline',
    default_args=default_args,
    description='Incrementally fetch, process, and upload DefiLlama pool APYs with metadata fetching, reset and do-not-upload flags',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 2, 8, 0, 0),
    catchup=False,
    max_active_runs=1,
) as dag:

    get_latest_rds_date_task = PythonOperator(
        task_id='get_latest_rds_date',
        python_callable=fetch_latest_rds_date,
    )

    fetch_metadata_task = PythonOperator(
        task_id='fetch_metadata',
        python_callable=fetch_metadata,
    )

    fetch_pool_yield_data_task = PythonOperator(
        task_id='fetch_pool_yield_data',
        python_callable=fetch_pool_yield_data,
    )

    process_apys_data_task = PythonOperator(
        task_id='process_apys_data',
        python_callable=process_apys_data,
        provide_context=True,
    )

    upload_to_rds_task = PythonOperator(
        task_id='upload_to_rds',
        python_callable=upload_to_rds,
    )

    # Set dependencies:
    # - get_latest_rds_date and fetch_metadata can run in parallel.
    # - fetch_pool_yield_data depends on fetch_metadata.
    # - process_apys_data then upload new rows.
    [get_latest_rds_date_task, fetch_metadata_task] >> fetch_pool_yield_data_task >> process_apys_data_task >> upload_to_rds_task
