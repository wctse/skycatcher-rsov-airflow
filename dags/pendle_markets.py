from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import requests
import time
import os
import logging
from typing import Optional, List, Dict

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# ───────────── feature toggles ─────────────
RESET_MODE   = False   # True  → ignore DB contents, pull everything
DO_NOT_UPLOAD = False  # True  → fetch & save CSV but skip DB insert

# ───────────── misc utilities ─────────────
logger = logging.getLogger("pendle_markets")
logger.setLevel(logging.INFO)

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)

def make_api_request(url: str,
                     method: str = "GET",
                     headers: Optional[dict] = None,
                     data: Optional[dict] = None,
                     max_retries: int = 3,
                     retry_delay: int = 60) -> dict:
    """HTTP request with basic throttle and 429 retry logic."""
    headers = headers or {"Content-Type": "application/json"}
    last_ts = getattr(make_api_request, "last_ts", 0)

    for attempt in range(max_retries):
        dt = time.time() - last_ts
        if dt < 0.1:                      # ~10 req/s guard
            time.sleep(0.1 - dt)

        try:
            r = requests.request(method, url, headers=headers, json=data, timeout=30)
            make_api_request.last_ts = time.time()

            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 and attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise AirflowException(f"{r.status_code}: {r.text[:200]}")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise AirflowException(e)

def safe_get(dct: Dict, keys: List[str], default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
        if cur is None and k != keys[-1]:
            return default
    return cur

# ───────────── constants ─────────────
BASE_URL   = "https://api-v2.pendle.finance/core/v1"
DATA_DIR   = "/tmp/pendle"
TABLE_NAME = "pendle_markets"
ensure_dir(DATA_DIR)

# RDS engine
conn = BaseHook.get_connection("rds_connection")
engine = create_engine(
    f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
)

# ───────────── task callables ─────────
def get_existing_markets(**_):
    """Return a set of addresses already stored in pendle_markets."""
    if RESET_MODE:
        return set()
    df = pd.read_sql(text(f"SELECT address FROM {TABLE_NAME}"), engine)
    return set(df["address"].str.lower())

def get_chain_ids(**_):
    """Fetch the list of supported chain IDs."""
    return make_api_request(f"{BASE_URL}/chains").get("chainIds", [])

def fetch_markets(**context):
    """Download markets for every chain and write only NEW ones to CSV."""
    existing = context["ti"].xcom_pull(task_ids="get_existing_markets") or set()
    chains   = context["ti"].xcom_pull(task_ids="get_chain_ids")        or []

    cols = [
        'address', 'chain_id', 'symbol', 'lp_decimals', 'expiry', 'protocol',
        'underlying_pool', 'pro_name', 'pro_symbol', 'asset_representation',
        'fee_rate', 'category_ids', 'timestamp',
        'pt_id', 'pt_chain_id', 'pt_address', 'pt_symbol', 'pt_decimals', 'pt_name',
        'yt_id', 'yt_chain_id', 'yt_address', 'yt_symbol', 'yt_decimals', 'yt_name',
        'sy_id', 'sy_chain_id', 'sy_address', 'sy_symbol', 'sy_decimals', 'sy_name',
        'aa_id', 'aa_chain_id', 'aa_address', 'aa_symbol', 'aa_decimals', 'aa_name',
        'ua_id', 'ua_chain_id', 'ua_address', 'ua_symbol', 'ua_decimals', 'ua_name',
        'pa_id', 'pa_chain_id', 'pa_address', 'pa_symbol', 'pa_decimals', 'pa_name',
    ]

    new_rows = []
    for cid in chains:
        skip, limit = 0, 100
        while True:
            url   = f"{BASE_URL}/{cid}/markets?limit={limit}&skip={skip}&select=all"
            page  = make_api_request(url)
            items = page.get("results", [])
            if not items:
                break

            for m in items:
                addr = m["address"].lower()
                if addr in existing:
                    continue
                new_rows.append({
                    'address': addr,
                    'chain_id': m.get('chainId'),
                    'symbol': m.get('symbol'),
                    'lp_decimals': safe_get(m, ['lp', 'decimals']),
                    'expiry': m.get('expiry'),
                    'protocol': m.get('protocol'),
                    'underlying_pool': m.get('underlyingPool'),
                    'pro_name': m.get('proName'),
                    'pro_symbol': m.get('proSymbol'),
                    'asset_representation': m.get('assetRepresentation'),
                    'fee_rate': safe_get(m, ['extendedInfo', 'feeRate']),
                    'category_ids': m.get('categoryIds'),
                    'timestamp': m.get('timestamp'),

                    'pt_id': safe_get(m, ['pt', 'id']),
                    'pt_chain_id': safe_get(m, ['pt', 'chainId']),
                    'pt_address': safe_get(m, ['pt', 'address']),
                    'pt_symbol': safe_get(m, ['pt', 'symbol']),
                    'pt_decimals': safe_get(m, ['pt', 'decimals']),
                    'pt_name': safe_get(m, ['pt', 'name']),

                    'yt_id': safe_get(m, ['yt', 'id']),
                    'yt_chain_id': safe_get(m, ['yt', 'chainId']),
                    'yt_address': safe_get(m, ['yt', 'address']),
                    'yt_symbol': safe_get(m, ['yt', 'symbol']),
                    'yt_decimals': safe_get(m, ['yt', 'decimals']),
                    'yt_name': safe_get(m, ['yt', 'name']),

                    'sy_id': safe_get(m, ['sy', 'id']),
                    'sy_chain_id': safe_get(m, ['sy', 'chainId']),
                    'sy_address': safe_get(m, ['sy', 'address']),
                    'sy_symbol': safe_get(m, ['sy', 'symbol']),
                    'sy_decimals': safe_get(m, ['sy', 'decimals']),
                    'sy_name': safe_get(m, ['sy', 'name']),

                    'aa_id': safe_get(m, ['accountingAsset', 'id']),
                    'aa_chain_id': safe_get(m, ['accountingAsset', 'chainId']),
                    'aa_address': safe_get(m, ['accountingAsset', 'address']),
                    'aa_symbol': safe_get(m, ['accountingAsset', 'symbol']),
                    'aa_decimals': safe_get(m, ['accountingAsset', 'decimals']),
                    'aa_name': safe_get(m, ['accountingAsset', 'name']),

                    'ua_id': safe_get(m, ['underlyingAsset', 'id']),
                    'ua_chain_id': safe_get(m, ['underlyingAsset', 'chainId']),
                    'ua_address': safe_get(m, ['underlyingAsset', 'address']),
                    'ua_symbol': safe_get(m, ['underlyingAsset', 'symbol']),
                    'ua_decimals': safe_get(m, ['underlyingAsset', 'decimals']),
                    'ua_name': safe_get(m, ['underlyingAsset', 'name']),

                    'pa_id': safe_get(m, ['basePricingAsset', 'id']),
                    'pa_chain_id': safe_get(m, ['basePricingAsset', 'chainId']),
                    'pa_address': safe_get(m, ['basePricingAsset', 'address']),
                    'pa_symbol': safe_get(m, ['basePricingAsset', 'symbol']),
                    'pa_decimals': safe_get(m, ['basePricingAsset', 'decimals']),
                    'pa_name': safe_get(m, ['basePricingAsset', 'name']),
                })

            skip += len(items)
            if len(items) < limit:
                break

    csv_path = os.path.join(DATA_DIR, "pendle_markets_incremental.csv")
    pd.DataFrame(new_rows, columns=cols).to_csv(csv_path, index=False)
    return csv_path

def upload_to_rds(**context):
    """Append new markets to RDS unless DO_NOT_UPLOAD is True.

    When DO_NOT_UPLOAD is True, print the would-be-inserted rows for review.
    """
    csv_path = context["ti"].xcom_pull(task_ids="fetch_markets")
    if not csv_path or not os.path.exists(csv_path):
        logger.info("nothing to upload")
        return

    df = pd.read_csv(csv_path)
    if df.empty:
        logger.info("CSV empty – nothing new")
        return

    if DO_NOT_UPLOAD:
        max_show = 1000                      # prevent gigantic log spam
        logger.info("[DO_NOT_UPLOAD] would insert %d rows:", len(df))
        logger.info("\n%s", df.head(max_show).to_string(index=False))
        if len(df) > max_show:
            logger.info("… and %d more rows not displayed", len(df) - max_show)
        return

    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    logger.info("inserted %d rows", len(df))

# ───────────── DAG ─────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="pendle_markets_pipeline",
    default_args=default_args,
    description=get_dag_config('pendle_markets')['description'],
    schedule_interval=get_schedule_interval('pendle_markets'),
    start_date=get_start_date('pendle_markets'),
    catchup=False,
    max_active_runs=1,
) as dag:

    get_existing_markets_task = PythonOperator(
        task_id="get_existing_markets",
        python_callable=get_existing_markets,
    )

    get_chain_ids_task = PythonOperator(
        task_id="get_chain_ids",
        python_callable=get_chain_ids,
    )

    fetch_markets_task = PythonOperator(
        task_id="fetch_markets",
        python_callable=fetch_markets,
    )

    upload_to_rds_task = PythonOperator(
        task_id="upload_to_rds",
        python_callable=upload_to_rds,
    )

    get_existing_markets_task >> get_chain_ids_task >> fetch_markets_task >> upload_to_rds_task
