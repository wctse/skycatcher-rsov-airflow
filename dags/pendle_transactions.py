"""
pendle_transactions.py   •   Airflow DAG (Python 3.8 syntax)

fetch_pendle_transactions  →  transform_pendle_transactions  →  upload_pendle_transactions
"""

import json, time, logging
from datetime import datetime, timedelta
from pathlib import Path
from datetime import timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# ─────────────────────────── CONFIG ──────────────────────────── #
RESET_MODE    = False   # ignore existing rows in RDS
DO_NOT_UPLOAD = False   # skip to_sql() (dry-run)

BASE_URL   = "https://api-v2.pendle.finance/core"
DATA_DIR   = Path("/tmp/pendle_tx")           ; DATA_DIR.mkdir(exist_ok=True)
RAW_CSV    = DATA_DIR / "pendle_raw.csv"
CUR_CSV    = DATA_DIR / "pendle_curated.csv"

RDS_TABLE   = "pendle_transactions"
RDS_CONN_ID = "rds_connection"

logger = logging.getLogger("pendle_tx")
logger.setLevel(logging.INFO)

# ─────────────────────── DB / HTTP HELPERS ───────────────────── #
def _engine():
    c = BaseHook.get_connection(RDS_CONN_ID)
    return create_engine(f"postgresql://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}")

def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(10)
            continue
        if r.status_code >= 400:
            raise AirflowException(f"{url} → {r.status_code}: {r.text[:120]}")
        return r.json()

def _latest_ts(chain_id: int) -> datetime:
    if RESET_MODE:
        return datetime(1970, 1, 1)

    sql = text(
        f'SELECT MAX("timestamp") AS ts '
        f'FROM {RDS_TABLE} '
        f'WHERE "chainId" = :cid'
    )                       #  ↑↑ quoted identifiers
    df = pd.read_sql(sql, _engine(), params={"cid": chain_id})
    return df.ts.iloc[0] if df.ts.iloc[0] is not None else datetime(1970, 1, 1)


# ───────────────────────── TASK 1 ─────────────────────────────── #
def fetch_pendle_transactions(**_):
    """Hit API for each chain, save combined raw CSV → RAW_CSV."""
    chains = _get(f"{BASE_URL}/v1/chains")["chainIds"]
    logger.info("Chains discovered: %s", chains)

    batches: List[pd.DataFrame] = []

    for cid in chains:
        start_dt  = _latest_ts(cid).replace(tzinfo=timezone.utc, microsecond=0) + timedelta(seconds=1) # start_timetsamp is inclusive
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")     # « 2025-01-01 00:00:00 »
        params: Dict[str, Any] = {
            "limit": 1000,
            "action": "SWAP_PT",
            "origin": "PENDLE_MARKET",
            "timestamp_start": start_str,
        }
        resume: Optional[str] = None
        first = True

        while True:
            if resume:
                params["resumeToken"] = resume
            payload = _get(f"{BASE_URL}/v4/{cid}/transactions", params=params)
            rows = payload.get("results", [])
            resume = payload.get("resumeToken")
            if not rows:
                break

            df = pd.json_normalize(rows)
            df["chainId"] = cid
            batches.append(df)
            if not resume:
                break

            if first:
                logger.info("Fetching transactions for chain %s (first batch)", cid)
                first = False
            else:
                logger.info("…fetching next batch of %s rows", len(rows))

    if not batches:
        logger.info("No new rows from any chain.")
        RAW_CSV.unlink(missing_ok=True)
        return

    pd.concat(batches, ignore_index=True).to_csv(RAW_CSV, index=False)
    logger.info("Fetched %s rows → %s", sum(len(b) for b in batches), RAW_CSV)

# ───────────────────────── TASK 2 ─────────────────────────────── #
def transform_pendle_transactions(**_):
    """Read RAW_CSV, explode JSON columns, drop unused cols, save CUR_CSV."""
    if not RAW_CSV.exists():
        logger.info("RAW_CSV not found – nothing to transform.")
        CUR_CSV.unlink(missing_ok=True)
        return

    df = pd.read_csv(RAW_CSV)

    def _explode(df_in: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
        if col not in df_in.columns:
            return df_in
        norm = (
            df_in[col]
            .apply(lambda v: json.loads(v.replace("'", '"')) if isinstance(v, str) else v)
            .fillna({})
            .pipe(pd.json_normalize)
            .add_prefix(prefix)
        )
        return pd.concat([df_in.drop(columns=[col]), norm], axis=1)

    df = (
        df.pipe(_explode, "inputs",  "inputs_")
          .pipe(_explode, "outputs", "outputs_")
          .drop(columns=[
              "id","market.id","market.chainId","market.expiry",
              "market.name","market.symbol","action","origin"
          ], errors="ignore")
    )

    for side in ("inputs_asset","outputs_asset"):
        if side in df.columns:
            df[side] = df[side].astype(str).str.split('-',1).str[-1]

    keep_cols = [
        "chainId",
        "timestamp",
        "market.address",
        "valuation.usd",
        "explicitSwapFeeSy"
    ]
    df = df[keep_cols]               # reorder & drop the rest

    df.to_csv(CUR_CSV, index=False)
    logger.info("Transformed → %s  (%s rows)", CUR_CSV, len(df))

# ───────────────────────── TASK 3 ─────────────────────────────── #
def upload_pendle_transactions(**_):
    """Append CUR_CSV to Postgres."""
    if not CUR_CSV.exists():
        logger.info("CUR_CSV not found – nothing to upload.")
        return
    df = pd.read_csv(CUR_CSV)

    if DO_NOT_UPLOAD:
        pd.set_option("display.max_rows", 1000)       # don’t truncate
        pd.set_option("display.max_columns", None)    # show all columns
        pd.set_option("display.width", None)          # avoid wrapping

        top_n    = df.head(500)
        bottom_n = df.tail(500) if len(df) > 500 else pd.DataFrame()

        logger.info(
            "\n— DO_NOT_UPLOAD MODE —"
            "\nTable shape : %s rows × %s columns"
            "\nColumn list : %s"
            "\n\nFirst 500 rows:\n%s"
            "\n\nLast 500 rows:\n%s",
            len(df), df.shape[1],
            ", ".join(df.columns),
            top_n.to_string(index=False),
            bottom_n.to_string(index=False) if not bottom_n.empty else "<less than 500 rows total>"
        )
        return
    
    df.to_sql(RDS_TABLE, _engine(), if_exists="append", index=False,
              method="multi", chunksize=1000)
    logger.info("Uploaded %s rows to %s", len(df), RDS_TABLE)

# ──────────────────────── DAG DEFINITION ──────────────────────── #
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="pendle_transactions",
    description=get_dag_config('pendle_transactions')['description'],
    schedule_interval=get_schedule_interval('pendle_transactions'),
    start_date=get_start_date('pendle_transactions'),
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_pendle_transactions_task    = PythonOperator(task_id="fetch_pendle_transactions",
                                python_callable=fetch_pendle_transactions)

    transform_pendle_transactions_task = PythonOperator(task_id="transform_pendle_transactions",
                                 python_callable=transform_pendle_transactions)

    upload_pendle_transactions_task   = PythonOperator(task_id="upload_pendle_transactions",
                                python_callable=upload_pendle_transactions)

    fetch_pendle_transactions_task >> transform_pendle_transactions_task >> upload_pendle_transactions_task
