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
import time
from requests.exceptions import RequestException
import concurrent.futures
import shutil
from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# Global Development Mode Flag
VERBOSE_MODE = False  # Print more information. Set to False for production
DO_NOT_UPLOAD = False
RESET_MODE = False # Set to True to upload all fetched date
TRIMMED_TESTING_MODE = False # Set to True to only process first 50 protocols

MAX_WORKERS = 5  # Avoid exceeding rate and memory limits

rds_conn = BaseHook.get_connection('rds_connection')
rds_engine = create_engine(
    f'postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}'
)

upload_to_suffix = "" # use in testing

# Configuration
API_ENDPOINT = 'https://pro-api.llama.fi'


# Configuration JSONs
TOKENS_CONFIG = {
    "eth": ["ETH", "WETH", "STETH", "WSTETH", "RETH", "WBETH", "METH", "FRXETH", 
            "SFRXETH", "CBETH", "OSETH", "ANKRETH", "LSETH", "OETH", "SWETH", 
            "STONE", "RSWETH", "EZETH", "WEETH", "RSETH", "PUFETH"],
    "sol": ["SOL", "MSOL", "BSOL", "JITOSOL", "JUPSOL", "INF", "BONKSOL", "DSOL", 
            "HSOL", "CLOCKSOL", "COMPASSSOL", "HUBSOL", "JUCYSOL", "LAINESOL", 
            "EDGESOL", "PICOSOL", "LST", "STSOL", "VSOL", "SUPERSOL", "STRONGSOL", 
            "STAKESOL", "PWRSOL", "PUMPKINSOL", "PATHSOL", "MANGOSOL", "LANTERNSOL", 
            "HAUSSOL", "STEPSOL", "HASOL", "PHASESOL", "APYSOL", "WENSOL", "BANXSOL", 
            "RKSOL", "NORDSOL", "DIGITSOL", "CAMAOSOL", "EONSOL", "LOTUSSOL", 
            "UPTSOL", "WIFSOL", "KUMASOL", "PINESOL", "DAINSOL", "USOL", "STAKRSOL", 
            "ICESOL", "RSOL", "DLGTSOL", "POLARSOL", "XSOL", "FPSOL", "LIFSOL", 
            "SPIKYSOL", "BURNSOL", "MALLOWSOL", "FUSESOL", "DUALSOL", "THUGSOL", 
            "DIGITALSOL", "GS", "BNSOL", "BBSOL"],
    "tao": ["TAO", "WTAO", "STTAO"],
    "near": ["NEAR", "WNEAR", "STNEAR", "LINEAR"],
    "apt": ["APT", "STAPT", "STHAPT", "THAPT", "AMAPT"],
    "avax": ["AVAX", "WAVAX", "SAVAX", "SAVAXI"],
    "ton": ["TON", "TONCOIN", "STTON", "TSTON"],
    "tia": ["TIA", "WTIA", "STTIA"],
    "sui": ["SUI"],
    "bnb": ["BNB", "WBNB"],
    "sei": ["SEI", "WSEI", "ISEI"],
    "sonic": ["S", "WS", "STS"],
    "hyperliquid": ["HYPE", "WHYPE", "STHYPE", "LHYPE"],
    "zora": ["ZORA"]
}

IGNORES_LIST = [
    {
        "filename": "21.co.json",
        "description": "Asset manager",
        "estimate": False
    },
    {
        "filename": "wrapped-bnb.json",
        "description": "Unrelated to DeFi",
        "estimate": False
    },
    {
        "filename": "ibc.json",
        "description": "A bridge between Cosmos chains, not put to use in DeFi",
        "estimate": False
    },
    {
        "filename": "uniswap-v3.json",
        "description": "Corrupted data",
        "estimate": True
    },
    {
        "filename": "venus-core-pool.json",
        "description": "Corrupted data on BNB",
        "estimate": True
    },
    {
        "filename": "alpaca-leveraged-yield-farming.json",
        "description": "Corrupted data on BNB",
        "estimate": True
    },
    {
        "filename": "orbit-bridge.json",
        "description": "Corrupted data on TON",
        "estimate": True
    }
]

ASSET_MAPPING = {
    'eth': {'chain': 'Ethereum', 'name': 'ethereum'},
    'sol': {'chain': 'Solana', 'name': 'solana'},
    'near': {'chain': 'Near', 'name': 'near'},
    'avax': {'chain': 'Avalanche', 'name': 'avalanche'},
    'bnb': {'chain': 'Binance', 'name': 'binance'},
    'apt': {'chain': 'Aptos', 'name': 'aptos'},
    'sui': {'chain': 'Sui', 'name': 'sui'},
    'ton': {'chain': 'Ton', 'name': 'ton'},
    'tia': {'chain': 'Celestia', 'name': 'celestia'},
    'sei': {'chain': 'Sei', 'name': 'sei'},
    'tao': {'chain': 'Bittensor', 'name': 'bittensor'},
    'sonic': {'chain': 'Sonic', 'name': 'sonic'},
    'hyperliquid': {'chain': 'Hyperliquid', 'name': 'hyperliquid'},
    'zora': {'chain': 'Zora', 'name': 'zora'}
}

CATEGORIES_TO_IGNORE = ['Liquid Staking', 'Liquid Restaking', 'CEX', 'Yield Aggregator']
CHAIN_SUFFIXS_TO_IGNORE = ['staking', 'pool2', 'borrowed', 'treasury', 'vesting']

INTERPOLATES = [
    {
        "asset": "bnb",
        "protocol": "pancakeswap-amm-v3",
        "start_date": "2023-06-27",
        "end_date": "2023-06-27",
        "description": "Missing data"
    },
    {
        "asset": "bnb",
        "protocol": "pancakeswap-amm-v3",
        "start_date": "2023-10-27",
        "end_date": "2023-10-28",
        "description": "Missing data"
    },
    {
        "asset": "bnb",
        "protocol": "pancakeswap-amm-v3",
        "start_date": "2024-01-28",
        "end_date": "2024-01-29",
        "description": "Missing data"
    },
    {
        "asset": "ton",
        "protocol": "hipo",
        "start_date": "2024-03-20",
        "end_date": "2024-03-28",
        "description": "Migration from Hipo to Hipo 2.0"
    },
    {
        "asset": "ton",
        "protocol": "ston.fi",
        "start_date": "2024-06-08",
        "end_date": "2024-06-10",
        "description": "Missing data"
    },
    {
        "asset": "tao",
        "protocol": "hatom-tao-liquid-staking",
        "start_date": "2024-08-01",
        "end_date": "2024-08-01",
        "description": "Missing data"
    },
    {
        "asset": "tao",
        "protocol": "hatom-lending",
        "start_date": "2024-08-01",
        "end_date": "2024-08-01",
        "description": "Missing data"
    },
    {
        "asset": "sui",
        "protocol": "cetus-amm",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "navi-lending",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "suilend",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "scallop-lend",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "kriya-amm",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "aftermath-amm",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "turbos",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "typus-dov",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "mole",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "omnibtc",
        "start_date": "2024-06-12",
        "end_date": "2024-06-12",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "cetus-amm",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "navi-lending",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "suilend",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "scallop-lend",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "kriya-amm",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "aftermath-amm",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "turbos",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "typus-dov",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "mole",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sui",
        "protocol": "omnibtc",
        "start_date": "2024-06-24",
        "end_date": "2024-06-24",
        "description": "Potential network issue"
    },
    {
        "asset": "sei",
        "protocol": "astroport",
        "start_date": "2023-08-29",
        "end_date": "2023-08-30",
        "description": "Missing data"
    },
    {
        "asset": "sei",
        "protocol": "astroport",
        "start_date": "2023-11-16",
        "end_date": "2023-11-16",
        "description": "Missing data"
    },
    {
        "asset": "sei",
        "protocol": "astroport",
        "start_date": "2024-01-26",
        "end_date": "2024-01-27",
        "description": "Missing data"
    },
    {
        "asset": "sei",
        "protocol": "astroport",
        "start_date": "2024-03-06",
        "end_date": "2024-03-10",
        "description": "Missing data"
    },
    {
        "asset": "near",
        "protocol": "ref-finance",
        "start_date": "2024-07-18",
        "end_date": "2024-08-05",
        "description": "Missing data"
    }
]

# Data directory
DATA_DIR = '/tmp/defillama'
TABLE_NAMES = [asset['name'] + '_deposit_value' for asset in ASSET_MAPPING.values()]

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
    
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, json=data, headers=headers, verify=False)
            
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

def epoch_to_utc(timestamp):
    """Convert epoch timestamp to UTC date string (YYYY-MM-DD)."""
    try:
        # Validate timestamp
        timestamp = int(timestamp)
        if not (0 < timestamp < 10000000000):
            print(f"Invalid timestamp: {timestamp}")
            return None
        # Convert to UTC date string
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d') # TODO: Fix the deprecation of utcfromtimestamp
    except (ValueError, TypeError) as e:
        print(f"Error converting timestamp {timestamp}: {e}")
        return None

# Dune functions
def fetch_dune_table_dates(**kwargs):
    try:
        api_key = load_api_key("api_key_dune")
        if not api_key:
            raise ValueError("Dune API key not found")
        
        dune = DuneClient(api_key)
        query = QueryBase(
            name="Sample Query",
            query_id="5270986" if not RESET_MODE else "5270978"
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

    print(f"Latest Dune dates: {latest_dune_dates_df.to_string()}")
    print(f"Latest RDS dates: {latest_rds_dates_df.to_string()}")

    data = {
        'dune_date': [],
        'rds_date': [],
        'min_of_max_dates': []
    }

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

            # Convert string dates to datetime objects for comparison
            dune_date = datetime.strptime(dune_date, '%Y-%m-%d') + timedelta(days=delta)
            rds_date = datetime.strptime(rds_date, '%Y-%m-%d') + timedelta(days=delta)
            
            # Get the earliest date to ensure we don't miss any data
            min_of_max_dates = min(dune_date, rds_date)

            # Append values to lists
            data['dune_date'].append(dune_date)
            data['rds_date'].append(rds_date) 
            data['min_of_max_dates'].append(min_of_max_dates)

        except Exception as e:
            raise AirflowException(f"Warning: Could not find dates for table {table_name}: {e}")

    # Create DataFrame with table_names as index
    results = pd.DataFrame(data, index=TABLE_NAMES)
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

def validate_protocols_file():
    protocols_file = os.path.join(DATA_DIR, 'protocols.json')
    if not os.path.exists(protocols_file):
        raise AirflowException(f"Protocols file not found: {protocols_file}")
    with open(protocols_file, 'r') as f:
        protocols = json.load(f)
    if not isinstance(protocols, list):
        raise AirflowException("Invalid protocols format. Expected array.")
    
def fetch_all_protocols(**kwargs):
    """Fetches the complete list of protocols from DeFiLlama."""
    try:
        api_key = load_api_key("api_key_defillama")
        if not api_key:
            raise ValueError("DefiLlama API key not found")

        ensure_dir(DATA_DIR)
        all_protocols_file = os.path.join(DATA_DIR, 'protocols.json')
        
        url = f"{API_ENDPOINT}/{api_key}/api/protocols"
        response = make_api_request(url)
        
        if not response:
            raise AirflowException("No data received from DeFiLlama protocols endpoint")
            
        # Extract only necessary fields
        simplified_protocols = [
            {
                'name': protocol['name'],
                'category': protocol.get('category', None),
                'slug': protocol['slug'],
                'misrepresentedTokens': protocol.get('misrepresentedTokens', False),
                'chainTvls': protocol['chainTvls']
            }
            for protocol in response
            if 'name' in protocol and 'slug' in protocol and protocol['category'] not in CATEGORIES_TO_IGNORE
        ]
        
        # Save simplified data
        with open(all_protocols_file, 'w') as f:
            json.dump(simplified_protocols, f, indent=2)
            
        print(f"Saved {len(simplified_protocols)} protocol entries to {all_protocols_file}")
        
        if TRIMMED_TESTING_MODE and simplified_protocols:
            print("TRIMMED_TESTING_MODE: Sample protocol data:")
            print(json.dumps(simplified_protocols[0], indent=2))
            
    except Exception as e:
        raise AirflowException(f"Error in fetch_all_protocols: {e}")

# Task functions
def fetch_asset_protocols(**kwargs):
    """Fetches protocol data for each token in each asset."""
    try:
        api_key = load_api_key("api_key_defillama")
        if not api_key:
            raise ValueError("DefiLlama API key not found")

        ensure_dir(DATA_DIR)
        token_protocols_dir = os.path.join(DATA_DIR, 'token_protocols')
        ensure_dir(token_protocols_dir)
        
        # Process assets
        assets_to_process = TOKENS_CONFIG.items()
        
        for asset, tokens in assets_to_process:
            print(f"\nProcessing asset: {asset}")
            asset_protocols = set()  # Using set to avoid duplicates
            
            # Process tokens
            tokens_to_process = tokens
            
            for token in tokens_to_process:
                print(f"Fetching data for token: {token}")
                url = f"{API_ENDPOINT}/{api_key}/api/tokenProtocols/{token}"
                
                response = make_api_request(url)
                if not response:
                    print(f"No data received for token {token}")
                    continue
                
                # Extract only protocol names, excluding CEXs
                for protocol in response:
                    if protocol.get('category') != 'CEX' and 'name' in protocol:
                        asset_protocols.add(protocol['name'])
                
                print(f"Processed {len(response)} protocols for token {token}")
            
            # Save protocols for this asset
            asset_file = os.path.join(token_protocols_dir, f"{asset}_protocols.json")
            with open(asset_file, 'w') as f:
                json.dump(list(asset_protocols), f, indent=2)
            
            print(f"Saved {len(asset_protocols)} protocol names for asset {asset}")
                
    except Exception as e:
        raise AirflowException(f"Error in fetch_asset_protocols: {e}")

def fetch_protocol_tvls(**kwargs):
    """Fetches TVL data for each protocol."""
    try:
        api_key = load_api_key("api_key_defillama")
        if not api_key:
            raise ValueError("DefiLlama API key not found")
        
        latest_dates_df = parse_latest_dates(0)
        deposit_value_tables = latest_dates_df[latest_dates_df.index.str.endswith('_deposit_value')]

        if deposit_value_tables.empty:
            raise AirflowException("No '_deposit_value' tables found in latest_dates.csv")

        # Find the earliest date among these tables
        latest_date = deposit_value_tables['min_of_max_dates'].min()
        start_timestamp = int(latest_date.timestamp())

        if VERBOSE_MODE:
            print(f"Using earliest latest date from '_deposit_value' tables: {latest_date}")
            print(f"Corresponding timestamp: {start_timestamp}")
        
        # Read protocols data
        protocols_file = os.path.join(DATA_DIR, 'protocols.json')
        with open(protocols_file, 'r') as f:
            protocol_mappings = json.load(f)

        if TRIMMED_TESTING_MODE:
            protocol_mappings = protocol_mappings[:50]
            print(f"TRIMMED_TESTING_MODE: Choosing first 50 protocol mappings: {protocol_mappings}")
        
        # Read the list of protocol names from token_protocols directory
        token_protocols_dir = os.path.join(DATA_DIR, 'token_protocols')
        all_protocol_names = set()
        protocol_metadata = {}  # New dictionary to store protocol metadata
        
        for file in os.listdir(token_protocols_dir):
            with open(os.path.join(token_protocols_dir, file), 'r') as f:
                protocol_names = json.load(f)
                all_protocol_names.update(protocol_names)
                # Initialize metadata for token-scanned protocols
                for name in protocol_names:
                    protocol_metadata[name] = {'chainTvlsOnly': False}

        # Add protocols that have TVL on relevant chains but weren't caught by token scanning
        for _, asset_info in ASSET_MAPPING.items():
            asset_chain = asset_info['chain']
            
            # Check each protocol in the full mapping
            for protocol in protocol_mappings:
                # Skip if already in our list
                if protocol['name'] in all_protocol_names:
                    continue
                
                # Check if protocol has TVL on the asset's chain
                if 'chainTvls' in protocol:
                    for chain_name in protocol['chainTvls'].keys():
                        if chain_name == asset_chain:
                            print(f"Adding {protocol['name']} due to TVL presence on {asset_chain}")
                            all_protocol_names.add(protocol['name'])
                            # Add metadata with chainTvlsOnly flag set to true
                            protocol_metadata[protocol['name']] = {'chainTvlsOnly': True}
                            break

        # Save the protocol metadata for use in process_tvls
        metadata_file = os.path.join(DATA_DIR, 'protocol_metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(protocol_metadata, f, indent=2)

        # Ensure protocol values directory exists
        protocol_values_dir = os.path.join(DATA_DIR, 'protocol_values')
        ensure_dir(protocol_values_dir)
        
        # Process protocols
        protocols_to_process = list(all_protocol_names)

        total_protocols = len(protocols_to_process)
        print(f"Total protocols to process: {total_protocols}")

        # Map protocol names to slugs
        protocol_name_to_slug = {p['name']: p['slug'] for p in protocol_mappings}

        # Function to fetch data for a single protocol
        def fetch_single_protocol(protocol_name):
            slug = protocol_name_to_slug.get(protocol_name)
            if not slug:
                print(f"No slug found for protocol {protocol_name}")
                return
            
            url = f"{API_ENDPOINT}/{api_key}/api/protocol/{slug}"
            response = make_api_request(url)
            
            if not response:
                print(f"No data received for protocol {slug}")
                return
            
            # Process and save TVL data with timestamp filtering
            protocol_file = os.path.join(protocol_values_dir, f"{slug}.json")
            processed_data = {}
            
            # Process chain TVLs
            for chain_name, chain_data in response.get('chainTvls', {}).items():
                # Skip chains with excluded terms
                if any(term in chain_name.lower() for term in CHAIN_SUFFIXS_TO_IGNORE):
                    continue
                
                # Process tokens and TVL data
                for entry in chain_data.get('tokensInUsd', []):
                    timestamp = int(entry.get('date', 0))
                    # Only process data after our start timestamp
                    if timestamp <= start_timestamp:
                        continue
                        
                    date_str = str(timestamp)
                    if date_str not in processed_data:
                        processed_data[date_str] = {'tokensInUsd': {}, 'tvl': {}}
                        
                    if chain_name not in processed_data[date_str]['tokensInUsd']:
                        processed_data[date_str]['tokensInUsd'][chain_name] = {}
                        
                    # Add token values
                    for token, value in entry.get('tokens', {}).items():
                        processed_data[date_str]['tokensInUsd'][chain_name][token] = value
                
                # Process TVL data
                for entry in chain_data.get('tvl', []):
                    timestamp = int(entry.get('date', 0))
                    # Only process data after our start timestamp
                    if timestamp <= start_timestamp:
                        continue
                        
                    date_str = str(timestamp)
                    if date_str not in processed_data:
                        processed_data[date_str] = {'tokensInUsd': {}, 'tvl': {}}
                    
                    processed_data[date_str]['tvl'][chain_name] = entry.get('totalLiquidityUSD', 0)
            
            if processed_data:  # Only save if we have new data
                # Sort by timestamp
                sorted_data = dict(sorted(processed_data.items()))
                with open(protocol_file, 'w') as f:
                    json.dump(sorted_data, f, indent=2)
            else:
                print(f"No new data found for {slug} after {latest_date.strftime('%Y-%m-%d')}")

        import time

        start_time = time.time()

        # Use ThreadPoolExecutor to fetch protocols in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for protocol_name in protocols_to_process:
                future = executor.submit(fetch_single_protocol, protocol_name)
                futures.append(future)
            
            # As futures complete, print progress
            for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                elapsed_time = time.time() - start_time
                remaining = total_protocols - idx
                rate = idx / elapsed_time if elapsed_time > 0 else 0
                estimated_remaining_time = remaining / rate if rate > 0 else float('inf')
                print(f"Processed {idx}/{total_protocols} protocols. "
                      f"Elapsed time: {elapsed_time:.2f}s. "
                      f"Estimated remaining time: {estimated_remaining_time:.2f}s.")

    except Exception as e:
        raise AirflowException(f"Error in fetch_protocol_tvls: {e}")

def process_tvls(**kwargs):
    """Processes TVL data and generates final CSV files."""
    try:
        protocol_values_dir = os.path.join(DATA_DIR, 'protocol_values')
        output_dir = os.path.join(DATA_DIR, 'asset_deposit_value')
        ensure_dir(output_dir)

        if VERBOSE_MODE:
            print("\nDEV MODE: Starting process_tvls")
            print(f"Protocol values directory exists: {os.path.exists(protocol_values_dir)}")
            print(f"Contents: {os.listdir(protocol_values_dir)}")

        # Read protocols data to check for misrepresented tokens
        protocols_file = os.path.join(DATA_DIR, 'protocols.json')
        with open(protocols_file, 'r') as f:
            protocols_data = json.load(f)

        # Create a lookup dict for faster access
        protocols_lookup = {p['slug']: p for p in protocols_data}

        # Read latest dates for assets
        latest_dates = parse_latest_dates(0)
        start_dates = latest_dates['min_of_max_dates']
        dune_dates = latest_dates['dune_date']
        rds_dates = latest_dates['rds_date']

        # Load protocol metadata
        metadata_file = os.path.join(DATA_DIR, 'protocol_metadata.json')
        if not os.path.exists(metadata_file):
            raise AirflowException("Protocol metadata file not found")
        
        with open(metadata_file, 'r') as f:
            protocol_metadata = json.load(f)

        # Process each asset
        assets_to_process = list(ASSET_MAPPING.items())[:1] if TRIMMED_TESTING_MODE else ASSET_MAPPING.items()

        for asset_code, asset_info in assets_to_process:
            asset_name = asset_info['name']
            print(f"\nProcessing asset: {asset_code} with info: {asset_info}")

            # Initialize per_protocol_data
            per_protocol_data = {}

            # Get list of protocol files
            protocol_files = [f for f in os.listdir(protocol_values_dir) if f.endswith('.json')]

            for protocol_file in protocol_files:
                protocol_slug = protocol_file.replace('.json', '')
                file_path = os.path.join(protocol_values_dir, protocol_file)
                
                # Load protocol data
                with open(file_path, 'r') as f:
                    raw_data = json.load(f)
                
                # Convert timestamps to dates and sort
                dated_data = {}
                for timestamp, data in raw_data.items():
                    date = epoch_to_utc(timestamp)
                    if date:
                        dated_data[date] = data

                # Remove the last date because it's incomplete
                if dated_data:
                    last_date = max(dated_data.keys())
                    dated_data.pop(last_date, None)

                # Check ignore status and misrepresented tokens
                ignore_entry = next((item for item in IGNORES_LIST 
                                   if item['filename'] == protocol_file), None)
                
                # Skip protocol if it's in ignore list and not to be estimated
                if ignore_entry and not ignore_entry['estimate']:
                    print(f"Skipping {protocol_file} as it's in ignore list and not for estimation")
                    continue
                
                protocol_info = protocols_lookup.get(protocol_slug, {})
                is_misrepresented = protocol_info.get('misrepresentedTokens', False)
                
                # Initialize per_protocol_data for this protocol
                per_protocol_data[protocol_slug] = {}

                # Process each date's data
                for date, data in dated_data.items():
                    if date not in per_protocol_data[protocol_slug]:
                        per_protocol_data[protocol_slug][date] = {'tokensInUsd': {}, 'estimatedTokensInUsd': {}}

                    # Check if this protocol should be estimated
                    protocol_meta = protocol_metadata.get(protocol_info.get('name', ''), {})
                    should_estimate = (
                        (ignore_entry and ignore_entry['estimate']) or 
                        is_misrepresented or
                        protocol_meta.get('chainTvlsOnly', False)  # Use the metadata flag
                    )

                    # Handle protocols that need estimation
                    if should_estimate:
                        # Handle estimated data
                        asset_chain = asset_info['chain']
                        
                        for chain, tvl_value in data.get('tvl', {}).items():
                            # Skip chains with excluded terms
                            if any(term in chain.lower() for term in CHAIN_SUFFIXS_TO_IGNORE):
                                continue
                                
                            # Only process TVL if it's from the asset's corresponding chain
                            if chain.lower() == asset_chain.lower() and tvl_value > 0:
                                if chain not in per_protocol_data[protocol_slug][date]['estimatedTokensInUsd']:
                                    per_protocol_data[protocol_slug][date]['estimatedTokensInUsd'][chain] = []
                                estimated_value = tvl_value / 2
                                per_protocol_data[protocol_slug][date]['estimatedTokensInUsd'][chain].append(estimated_value)

                                if VERBOSE_MODE:
                                    print(f"Added estimated value {estimated_value} for {date} on chain {chain} "
                                          f"(Reason: {'Ignore list' if ignore_entry else 'Misrepresented' if is_misrepresented else 'No TokensInUsd data'})")

                    else:
                        # Handle regular token data
                        tokens_in_usd = data.get('tokensInUsd', {})
                        for chain, tokens_data in tokens_in_usd.items():
                            # Skip chains with excluded terms
                            if any(term in chain.lower() for term in CHAIN_SUFFIXS_TO_IGNORE):
                                continue
                                
                            # Sum up values for matching tokens
                            total_value = 0
                            for token, value in tokens_data.items():
                                if token in TOKENS_CONFIG[asset_code]:
                                    total_value += value
                                    if VERBOSE_MODE:
                                        print(f"Added token value {value} for {token} on {date} (chain: {chain})")

                            if total_value > 0:
                                if chain not in per_protocol_data[protocol_slug][date]['tokensInUsd']:
                                    per_protocol_data[protocol_slug][date]['tokensInUsd'][chain] = []
                                per_protocol_data[protocol_slug][date]['tokensInUsd'][chain].append(total_value)

                # Apply interpolation to this protocol's data
                per_protocol_data[protocol_slug] = apply_interpolation(
                    per_protocol_data[protocol_slug], asset_code, protocol_slug
                )

            # After processing all protocols, aggregate data
            processed_data = {}

            table_name = f"{asset_name}_deposit_value"
            start_date = start_dates.loc[table_name] if table_name in start_dates.index else None

            for protocol_slug, protocol_dates in per_protocol_data.items():
                for date, data in protocol_dates.items():
                    if datetime.strptime(date, '%Y-%m-%d') <= start_date:
                        continue 

                    if date not in processed_data:
                        processed_data[date] = {}

                    # Aggregate data per chain
                    for chain in data.get('tokensInUsd', {}):
                        if chain not in processed_data[date]:
                            processed_data[date][chain] = {'tokens_usd': 0, 'estimated_tokens_usd': 0}
                        processed_data[date][chain]['tokens_usd'] += sum(data['tokensInUsd'][chain])

                    for chain in data.get('estimatedTokensInUsd', {}):
                        if chain not in processed_data[date]:
                            processed_data[date][chain] = {'tokens_usd': 0, 'estimated_tokens_usd': 0}
                        processed_data[date][chain]['estimated_tokens_usd'] += sum(data['estimatedTokensInUsd'][chain])

            # Remove the last date because it's incomplete
            if processed_data:
                last_date = max(processed_data.keys())
                processed_data.pop(last_date, None)

            if VERBOSE_MODE:
                print(f"Filtered processed data: {processed_data}")

            if not processed_data:
                print(f"No new data for asset {asset_name} after {start_date}")
                continue

            # Convert to DataFrame format
            csv_data = []
            for date in sorted(processed_data.keys()):
                for chain, values in processed_data[date].items():
                    total_value = values['tokens_usd'] + values['estimated_tokens_usd']

                    if total_value > 0:  # Only include entries with values
                        csv_data.append({
                            'date': date,
                            'chain': chain,
                            'tokens_usd': values['tokens_usd'],
                            'estimated_tokens_usd': values['estimated_tokens_usd'],
                            'total_deposit_value': total_value
                        })

            if VERBOSE_MODE:
                print(f"\nProcessed data summary:")
                print(f"Total records: {len(csv_data)}")
                if csv_data:
                    print("Sample first entry:", csv_data[0])
                    print("Sample last entry:", csv_data[-1])
                    print("Unique chains:", len(set(record['chain'] for record in csv_data)))
                    print("Date range:", min(record['date'] for record in csv_data), 
                          "to", max(record['date'] for record in csv_data))

            # Save to CSV
            df = pd.DataFrame(csv_data)

            df_dune = df[df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d') >= dune_dates.loc[asset_name + '_deposit_value'])]
            csv_file_dune = os.path.join(output_dir, f"{asset_info['name']}_dune.csv")
            df_dune.to_csv(csv_file_dune, index=False)

            df_rds = df[df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d') >= rds_dates.loc[asset_name + '_deposit_value'])]
            csv_file_rds = os.path.join(output_dir, f"{asset_info['name']}_rds.csv")
            df_rds.to_csv(csv_file_rds, index=False)
            
            print(f"Saved CSV files for {asset_code} to {csv_file_dune} and {csv_file_rds}")

            if VERBOSE_MODE:
                print("\nDataFrame info:")
                print(df.info())
                print("\nSummary by chain:")
                print(df.groupby('chain')['total_deposit_value'].sum())

    except Exception as e:
        print(f"Detailed error: {str(e)}")
        raise AirflowException(f"Error in process_tvls: {e}")

def apply_interpolation(protocol_data, asset_code, protocol_slug):
    """Applies interpolation to protocol data if specified in the INTERPOLATES list."""
    # Find all interpolation items matching the current asset and protocol
    interpolation_items = [item for item in INTERPOLATES if item['asset'] == asset_code and item['protocol'] == protocol_slug]
    if not interpolation_items:
        # No interpolation needed for this protocol
        return protocol_data  # Return the data unchanged

    # Proceed with interpolation...
    for interpolation_item in interpolation_items:
        start_date_str = interpolation_item['start_date']
        end_date_str = interpolation_item['end_date']

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Convert date strings to Date objects
        date_strings = list(protocol_data.keys())
        date_objects = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in date_strings]
        date_objects.sort()

        # Find prevDate (date before startDate)
        prev_date = None
        for d in reversed(date_objects):
            if d < start_date:
                prev_date = d
                break

        # Find nextDate (date after endDate)
        next_date = None
        for d in date_objects:
            if d > end_date:
                next_date = d
                break

        if not prev_date or not next_date:
            print(f"Cannot perform interpolation for {asset_code}/{protocol_slug} from {start_date_str} to {end_date_str} due to insufficient data")
            continue

        prev_date_str = prev_date.strftime('%Y-%m-%d')
        next_date_str = next_date.strftime('%Y-%m-%d')

        # Get chain data for prevDate and nextDate
        prev_tokens_data = protocol_data.get(prev_date_str, {}).get('tokensInUsd', {})
        next_tokens_data = protocol_data.get(next_date_str, {}).get('tokensInUsd', {})

        prev_estimated_data = protocol_data.get(prev_date_str, {}).get('estimatedTokensInUsd', {})
        next_estimated_data = protocol_data.get(next_date_str, {}).get('estimatedTokensInUsd', {})

        # Get all unique chains from both dates and categories
        all_chains = set(prev_tokens_data.keys()) | set(next_tokens_data.keys()) | set(prev_estimated_data.keys()) | set(next_estimated_data.keys())

        date_difference = (next_date - prev_date).days

        # Generate dates between startDate and endDate
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            days_since_prev = (current_date - prev_date).days
            interpolation_fraction = days_since_prev / date_difference if date_difference != 0 else 0

            if date_str not in protocol_data:
                protocol_data[date_str] = {'tokensInUsd': {}, 'estimatedTokensInUsd': {}}

            for chain in all_chains:
                # Interpolate tokensInUsd if data exists
                prev_chain_values = protocol_data.get(prev_date_str, {}).get('tokensInUsd', {}).get(chain, [])
                next_chain_values = protocol_data.get(next_date_str, {}).get('tokensInUsd', {}).get(chain, [])

                prev_chain_value = sum(prev_chain_values) if prev_chain_values else 0
                next_chain_value = sum(next_chain_values) if next_chain_values else 0

                interpolated_chain_value = prev_chain_value + (next_chain_value - prev_chain_value) * interpolation_fraction

                if interpolated_chain_value > 0:
                    if chain not in protocol_data[date_str]['tokensInUsd']:
                        protocol_data[date_str]['tokensInUsd'][chain] = []
                    protocol_data[date_str]['tokensInUsd'][chain].append(interpolated_chain_value)

                # Interpolate estimatedTokensInUsd if data exists
                prev_estimated_values = protocol_data.get(prev_date_str, {}).get('estimatedTokensInUsd', {}).get(chain, [])
                next_estimated_values = protocol_data.get(next_date_str, {}).get('estimatedTokensInUsd', {}).get(chain, [])

                prev_estimated_value = sum(prev_estimated_values) if prev_estimated_values else 0
                next_estimated_value = sum(next_estimated_values) if next_estimated_values else 0

                interpolated_estimated_value = prev_estimated_value + (next_estimated_value - prev_estimated_value) * interpolation_fraction

                if interpolated_estimated_value > 0:
                    if chain not in protocol_data[date_str]['estimatedTokensInUsd']:
                        protocol_data[date_str]['estimatedTokensInUsd'][chain] = []
                    protocol_data[date_str]['estimatedTokensInUsd'][chain].append(interpolated_estimated_value)

            current_date += timedelta(days=1)

    return protocol_data

def upload_to_dune(**kwargs):
    """Uploads processed data to Dune Analytics."""
    
    try:
        asset_deposit_dir = os.path.join(DATA_DIR, 'asset_deposit_value')
        
        for asset_code, asset_info in ASSET_MAPPING.items():
            csv_file = os.path.join(asset_deposit_dir, f"{asset_info['name']}_dune.csv")
            
            if not os.path.exists(csv_file):
                print(f"CSV file not found for {asset_code}: {csv_file}")
                continue
            
            df = pd.read_csv(csv_file)
            table_name = f"{asset_info['name']}_deposit_value"
            
            if DO_NOT_UPLOAD:
                print(f"\nDO NOT UPLOAD: Data to be uploaded for {asset_code}:")
                print(df.to_string())
                print(f"Total rows: {len(df)}")
            else:
                response = insert_df_to_dune(df, table_name)
                print(f"Uploaded {asset_code} data to Dune. Response: {response}")
        
    except Exception as e:
        raise AirflowException(f"Error in upload_to_dune: {e}")

def upload_to_rds(**kwargs):
    try:
        asset_deposit_dir = os.path.join(DATA_DIR, 'asset_deposit_value')
        
        for asset_code, asset_info in ASSET_MAPPING.items():
            csv_file = os.path.join(asset_deposit_dir, f"{asset_info['name']}_rds.csv")
            
            if not os.path.exists(csv_file):
                print(f"CSV file not found for {asset_code}: {csv_file}")
                continue
                
            df = pd.read_csv(csv_file)
            table_name = f"{asset_info['name']}_deposit_value"
            
            if DO_NOT_UPLOAD:
                print(f"\nDO NOT UPLOAD: Data to be uploaded to RDS for {asset_code}:")
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
    'sov_defillama',
    default_args=default_args,
    description=get_dag_config('sov_defillama')['description'],
    schedule_interval=timedelta(days=3), # schedule_interval=get_schedule_interval('sov_defillama'),
    start_date=datetime(2025, 6, 7, 0, 1, 0), # get_start_date('sov_defillama'),
    catchup=False
)

clear_data_task = PythonOperator(
    task_id='clear_data',
    python_callable=clear_data_directory,
    dag=dag,
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

fetch_asset_protocols_task = PythonOperator(
    task_id='fetch_asset_protocols',
    python_callable=fetch_asset_protocols,
    dag=dag,
)

fetch_all_protocols_task = PythonOperator(
    task_id='fetch_all_protocols',
    python_callable=fetch_all_protocols,
    dag=dag,
)

fetch_protocol_tvls_task = PythonOperator(
    task_id='fetch_protocol_tvls',
    python_callable=fetch_protocol_tvls,
    dag=dag,
)

process_tvls_task = PythonOperator(
    task_id='process_tvls',
    python_callable=process_tvls,
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

clear_data_task >> [fetch_rds_table_dates_task, fetch_dune_table_dates_task, fetch_all_protocols_task]
fetch_rds_table_dates_task >> fetch_asset_protocols_task
fetch_dune_table_dates_task >> fetch_asset_protocols_task
[fetch_all_protocols_task, fetch_asset_protocols_task] >> fetch_protocol_tvls_task >> process_tvls_task >> [upload_to_dune_task, upload_to_rds_task]