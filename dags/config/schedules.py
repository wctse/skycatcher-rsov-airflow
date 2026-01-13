"""
Centralized configuration for all DAG schedules and start dates.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, Union

# Base configuration
START_DATE = datetime(2025, 7, 7, 0, 0, 0)  # Base start date
INTERVAL = timedelta(days=7)

# Optional per-DAG start time overrides (UTC)
CUSTOM_START_TIMES = {
    # Run Glassnode ingestion at 09:00 UTC
    "glassnode_metrics": (9, 0),
    # Run CoinGecko ingestion at 09:00 UTC
    "coingecko_metrics": (9, 0),
}

# Optional per-DAG schedule interval overrides
CUSTOM_SCHEDULE_INTERVALS = {
    "glassnode_metrics": "0 9 * * *",
    "coingecko_metrics": "0 9 * * *",
}

# Define DAGs with their execution times in minutes
# The order here determines the scheduling order.
# DAGs can be grouped in lists to run concurrently.
DAG_DEFINITIONS = [
    [
        ('sov_taostats_subnets', 10, 'A DAG for fetching, processing and uploading subnet data'),
        ('sov_taostats_network', 5, 'A DAG for fetching, processing and uploading network stats'),
        ('sov_taostats_prices', 5, 'A DAG for fetching, processing and uploading TAO price data'),
        ('sov_artemis', 5, 'SOV Artemis data pipeline'),
        ('pendle_transactions', 3, 'Pendle transactions data pipeline'),
        ('pendle_markets', 3, 'Pendle markets data pipeline'),
        ('dune_to_rds', 5, 'Dune data to RDS pipeline'),
        ('sov_staking_rewards', 15, 'A DAG for processing staking rewards data'),
    ],
    ('pendle_pt_yt_prices', 20, 'Pendle PT/YT prices data pipeline'),
    ('sov_taostats_subnet_pools', 60, 'A DAG for fetching, processing and uploading pool history data'),
    ('btc_amounts', 15, 'BTC amounts data pipeline'),
    ('stablecoin_amounts', 15, 'Stablecoins amounts data pipeline'),
    ('apys_defillama', 45, 'APYs from DefiLlama pipeline'),
    ('sov_defillama', 60, 'SOV DefiLlama data pipeline'),
    [
        ('cryptoquant_metrics', 45, 'CryptoQuant metrics ingestion pipeline'),
        ('glassnode_metrics', 45, 'GlassNode metrics ingestion pipeline'),
        ('coingecko_metrics', 30, 'CoinGecko OHLC ingestion pipeline'),
    ],
]

# Generate DAG configurations with calculated start times
DAG_CONFIGS: Dict[str, Dict[str, Any]] = {}
current_time = START_DATE

for dag_group in DAG_DEFINITIONS:
    if isinstance(dag_group, tuple):
        # Handle single DAG definition
        dag_group = [dag_group]

    max_duration = 0
    for dag_id, duration_minutes, description in dag_group:
        DAG_CONFIGS[dag_id] = {
            'schedule_interval': INTERVAL,
            'start_date': current_time,
            'description': description,
            'duration_minutes': duration_minutes
        }
        if duration_minutes > max_duration:
            max_duration = duration_minutes
    
    # Move to the next time slot
    current_time += timedelta(minutes=max_duration)

# Apply start time overrides
for dag_id, (hour, minute) in CUSTOM_START_TIMES.items():
    if dag_id in DAG_CONFIGS:
        start = DAG_CONFIGS[dag_id]["start_date"]
        DAG_CONFIGS[dag_id]["start_date"] = start.replace(hour=hour, minute=minute, second=0, microsecond=0)

# Apply schedule interval overrides
for dag_id, interval in CUSTOM_SCHEDULE_INTERVALS.items():
    if dag_id in DAG_CONFIGS:
        DAG_CONFIGS[dag_id]["schedule_interval"] = interval

def get_schedule_interval(dag_id: str) -> Union[timedelta, str]:
    return DAG_CONFIGS[dag_id]['schedule_interval']

def get_start_date(dag_id: str) -> datetime:
    return DAG_CONFIGS[dag_id]['start_date']

def get_dag_config(dag_id: str) -> dict:
    config = DAG_CONFIGS[dag_id].copy()
    config.pop('duration_minutes', None)
    return config