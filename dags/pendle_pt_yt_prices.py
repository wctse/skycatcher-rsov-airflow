"""
pendle_pt_yt_prices.py
─────────────────────────────────────────────────────────────────────────────
Fetch hourly PT / YT OHLCV for every Pendle market and append to Postgres.
"""

import json, time, logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from config.schedules import get_schedule_interval, get_start_date, get_dag_config

# ───────────────────────── CONFIG ───────────────────────── #
RESET_MODE    = False
DO_NOT_UPLOAD = False

BASE_URL      = "https://api-v2.pendle.finance/core"
DATA_DIR      = Path("/tmp/pendle_pt_yt"); DATA_DIR.mkdir(exist_ok=True)
RAW_CSV       = DATA_DIR / "pt_yt_raw.csv"
CURATED_CSV   = DATA_DIR / "pt_yt_curated.csv"

RDS_TABLE     = "pendle_pt_yt_prices"
RDS_CONN_ID   = "rds_connection"

RETRY_DELAY   = 60
MAX_RETRIES   = 3
MARKETS_LIMIT = 100

logger = logging.getLogger("pendle_ptyt")
logger.setLevel(logging.INFO)

# ────────────────────── DB / HTTP HELPERS ─────────────────── #
def _engine():
    c = BaseHook.get_connection(RDS_CONN_ID)
    return create_engine(f"postgresql://{c.login}:{c.password}@{c.host}:{c.port}/{c.schema}")

def _get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise AirflowException(f"{url} → {r.status_code}: {r.text[:120]}")
    return r.json()

# ─────────────────── helper to fetch a token’s OHLCV ─────────────────── #
def fetch_hourly_ohlcv(chain_id: int, token_addr: str, ts_start: str) -> Tuple[List[str], List[float], List[float], List[float], List[float]]:
    url = f"{BASE_URL}/v4/{chain_id}/prices/{token_addr}/ohlcv?time_frame=hour&timestamp_start={ts_start}"
    retries = 0
    
    while retries <= MAX_RETRIES:
        try:
            r = requests.get(url, timeout=30)

            if r.status_code == 200:
                csv_str = r.json().get("results", "").lstrip()
                rows = [ln for ln in csv_str.splitlines() if ln.strip()]

                if len(rows) <= 1:
                    return [], [], [], [], []
                
                ts, o, h, l, c = [], [], [], [], []
                for ln in rows[1:]:                 # skip header row
                    t, op, hi, lo, cl, _ = ln.split(",")

                    # ── skip sparse rows that contain any blank numeric field ──
                    if "" in (op, hi, lo, cl):
                        logger.debug("Skipping sparse OHLCV row: %s", ln)
                        continue

                    ts.append(t)
                    o.append(float(op))
                    h.append(float(hi))
                    l.append(float(lo))
                    c.append(float(cl))
                    
                return ts, o, h, l, c
            
            if r.status_code == 429:
                logger.warning("429 for %s on chain %s – sleep %s", token_addr, chain_id, RETRY_DELAY)
                time.sleep(RETRY_DELAY); retries += 1; continue
            
            logger.error("HTTP %s at %s : %s", r.status_code, token_addr, r.text[:120])
            return [], [], [], [], []
        
        except requests.exceptions.RequestException as e:
            logger.error("Network error %s", e); return [], [], [], [], []
        
    return [], [], [], [], []

# ───────────────────── TASK 1a: market metadata ────────────────────── #
def fetch_pendle_market_metadata(ti, **_):
    """Pull every market once and push a list of {chainId, lpt, pt, yt} dicts."""
    chains = _get(f"{BASE_URL}/v1/chains")["chainIds"]
    logger.info("Chains: %s", chains)

    markets_meta: List[Dict[str, Any]] = []

    for cid in chains:
        skip = 0
        while True:
            url = f"{BASE_URL}/v1/{cid}/markets?limit={MARKETS_LIMIT}&skip={skip}"
            payload = _get(url)
            results = payload.get("results", [])
            if not results:
                break
            for m in results:
                lpt = m.get("address"); pt = m.get("pt", {}).get("address"); yt = m.get("yt", {}).get("address")
                if all([lpt, pt, yt]):
                    markets_meta.append({"chainId": cid, "lpt": lpt, "pt": pt, "yt": yt})
            if len(results) < MARKETS_LIMIT:
                break
            skip += MARKETS_LIMIT

    logger.info("Fetched metadata for %s markets", len(markets_meta))
    ti.xcom_push(key="markets_meta", value=json.dumps(markets_meta))

# ─────────── TASK 1b: fetch latest hours in bulk ────────────
def fetch_latest_hours(ti, **_):
    """
    Read one row per market from Postgres:
        address | max_hour
    Push a {address: max_hour} dict via XCom.
    """
    if RESET_MODE:
        ti.xcom_push(key="latest_hours", value={})
        return

    sql = text(f'''
        SELECT "address", MAX("hour") AS max_hour
        FROM {RDS_TABLE}
        GROUP BY "address";
    ''')
    df = pd.read_sql(sql, _engine())

    latest_map = {
        row.address: row.max_hour.isoformat()
        for row in df.itertuples(index=False)
        if row.max_hour is not None
    }
    ti.xcom_push(key="latest_hours", value=json.dumps(latest_map))

    min_ts = min(row.max_hour for row in df.itertuples(index=False) if row.max_hour is not None)
    max_ts = max(row.max_hour for row in df.itertuples(index=False) if row.max_hour is not None)
    logger.info("Fetched latest hours for %s markets (min: %s, max: %s)", 
                len(latest_map), min_ts, max_ts)

# ───────────────────── TASK 2: fetch hourly prices ─────────────────── #
def fetch_pt_yt_hourly_prices(ti, **_):
    meta_json = ti.xcom_pull(task_ids="fetch_pendle_market_metadata", key="markets_meta")
    markets_meta: List[Dict[str, Any]] = json.loads(meta_json or "[]")

    latest_map_json = ti.xcom_pull(task_ids="fetch_latest_hours", key="latest_hours")
    latest_map = json.loads(latest_map_json or "{}")

    raw_rows: List[Dict[str, Any]] = []
    for meta in markets_meta:
        cid, lpt, pt, yt = meta["chainId"], meta["lpt"], meta["pt"], meta["yt"]
        last_hour = (
            datetime.fromisoformat(latest_map[lpt])
            if lpt in latest_map
            else datetime(1970, 1, 1, tzinfo=timezone.utc)
        )
        ts_start = (last_hour + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

        ts_pt, o_pt, h_pt, l_pt, c_pt = fetch_hourly_ohlcv(cid, pt, ts_start)
        if not ts_pt:
            continue
        ts_yt, o_yt, h_yt, l_yt, c_yt = fetch_hourly_ohlcv(cid, yt, ts_start)
        if not ts_yt:
            continue

        pt_map = {int(t): (o, h, l, c) for t, o, h, l, c in zip(ts_pt, o_pt, h_pt, l_pt, c_pt)}
        yt_map = {int(t): (o, h, l, c) for t, o, h, l, c in zip(ts_yt, o_yt, h_yt, l_yt, c_yt)}

        for epoch in sorted(set(pt_map).intersection(yt_map)):
            po, ph, pl, pc = pt_map[epoch]
            yo, yh, yl, yc = yt_map[epoch]
            raw_rows.append({
                "chainId":  cid,
                "address":  lpt,
                "hour":     datetime.fromtimestamp(epoch, tz=timezone.utc),
                "pt_open":  po, "pt_high": ph, "pt_low": pl, "pt_close": pc,
                "yt_open":  yo, "yt_high": yh, "yt_low": yl, "yt_close": yc,
            })
        
        logger.info(f"Fetched {len(raw_rows)} rows for LP token {lpt}")

    if not raw_rows:
        logger.info("Nothing new to fetch."); RAW_CSV.unlink(missing_ok=True)
        return
    
    pd.DataFrame(raw_rows).to_csv(RAW_CSV, index=False)
    logger.info("Fetched %s rows → %s", len(raw_rows), RAW_CSV)

# ───────────────────── TASK 3: transform ───────────────────── #
def transform_pt_yt_hourly_prices(**_):
    if not RAW_CSV.exists():
        logger.info("RAW_CSV not found – nothing to transform."); CURATED_CSV.unlink(missing_ok=True)
        return
    
    df = pd.read_csv(RAW_CSV, parse_dates=["hour"])
    df.to_csv(CURATED_CSV, index=False)
    logger.info("Transformed → %s (%s rows)", CURATED_CSV, len(df))

# ───────────────────── TASK 4: upload ─────────────────────── #
def upload_pt_yt_hourly_prices(**_):
    if not CURATED_CSV.exists():
        logger.info("CUR_CSV not found – nothing to upload."); return
    df = pd.read_csv(CURATED_CSV, parse_dates=["hour"])
    df.rename(columns={"chainId": "chain_id"}, inplace=True)

    if DO_NOT_UPLOAD:
        pd.set_option("display.max_columns", None)
        logger.info("\n— DO_NOT_UPLOAD MODE —\n%s\n...\n%s",
                    df.head(10).to_string(index=False),
                    df.tail(10).to_string(index=False))
        
        logger.info("Supposed to upload %s rows to %s, with columns %s",
                    len(df), RDS_TABLE, ", ".join(df.columns.to_list()))
        return
    
    df.to_sql(RDS_TABLE, _engine(), if_exists="append", index=False,
              method="multi", chunksize=1000)
    logger.info("Uploaded %s rows to %s", len(df), RDS_TABLE)

# ───────────────────── DAG DEFINITION ─────────────────────── #
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="pendle_pt_yt_prices",
    description=get_dag_config('pendle_pt_yt_prices')['description'],
    schedule_interval=get_schedule_interval('pendle_pt_yt_prices'),
    start_date=get_start_date('pendle_pt_yt_prices'),
    catchup=False,
    max_active_runs=1,
) as dag:

    fetch_markets_task = PythonOperator(
        task_id="fetch_pendle_market_metadata",
        python_callable=fetch_pendle_market_metadata
    )

    fetch_latest_hours_task = PythonOperator(
        task_id="fetch_latest_hours",
        python_callable=fetch_latest_hours
    )

    fetch_prices_task = PythonOperator(
        task_id="fetch_pt_yt_hourly_prices",
        python_callable=fetch_pt_yt_hourly_prices,
        provide_context=True
    )

    transform_prices_task = PythonOperator(
        task_id="transform_pt_yt_hourly_prices",
        python_callable=transform_pt_yt_hourly_prices
    )

    upload_to_rds_task = PythonOperator(
        task_id="upload_pt_yt_hourly_prices",
        python_callable=upload_pt_yt_hourly_prices
    )

    [fetch_markets_task, fetch_latest_hours_task] >> fetch_prices_task >> transform_prices_task >> upload_to_rds_task
