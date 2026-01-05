"""Airflow DAG for ingesting CoinGecko OHLC price data into coin_metrics."""

import logging
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from config.schedules import get_dag_config, get_schedule_interval, get_start_date


logger = logging.getLogger("coingecko_metrics")
logger.setLevel(logging.INFO)

# API settings
COINGECKO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
COINGECKO_VS_CURRENCY = "usd"
COINGECKO_API_KEY_VAR = "api_key_coingecko"
# Allowed day buckets per API docs for OHLC endpoint
COINGECKO_ALLOWED_DAYS = [1, 7, 14, 30, 90, 180, 365]
COINGECKO_DEFAULT_DAYS = "max"  # fallback when no history exists

# Assets to ingest (symbol -> CoinGecko id)
COINGECKO_COINS: Sequence[Dict[str, str]] = [
    {"asset_symbol": "btc", "coingecko_id": "bitcoin"},
    {"asset_symbol": "eth", "coingecko_id": "ethereum"},
    {"asset_symbol": "sol", "coingecko_id": "solana"},
    {"asset_symbol": "aave", "coingecko_id": "aave"},
    {"asset_symbol": "uni", "coingecko_id": "uniswap"},
    {"asset_symbol": "hype", "coingecko_id": "hyperliquid"},
    {"asset_symbol": "pendle", "coingecko_id": "pendle"},
    {"asset_symbol": "ethfi", "coingecko_id": "ether-fi"},
    {"asset_symbol": "lido", "coingecko_id": "lido-dao"},
    {"asset_symbol": "tao", "coingecko_id": "bittensor"},
    {"asset_symbol": "aero", "coingecko_id": "aerodrome-finance"},
    {"asset_symbol": "sui", "coingecko_id": "sui"},
    {"asset_symbol": "bnb", "coingecko_id": "binancecoin"},
    {"asset_symbol": "link", "coingecko_id": "chainlink"},
    {"asset_symbol": "ena", "coingecko_id": "ethena"},
    {"asset_symbol": "mon", "coingecko_id": "monad"},
    {"asset_symbol": "zec", "coingecko_id": "zcash"},
    {"asset_symbol": "gs", "coingecko_id": "gammaswap"},
    {"asset_symbol": "pnkstr", "coingecko_id": "punkstrategy"},
    {"asset_symbol": "sky", "coingecko_id": "sky"},
    {"asset_symbol": "morpho", "coingecko_id": "morpho"},
    {"asset_symbol": "virtual", "coingecko_id": "virtual-protocol"},
    {"asset_symbol": "almanak", "coingecko_id": "almanak"},
    {"asset_symbol": "giza", "coingecko_id": "giza"},
]

# Target table/source
TABLE_NAME = "coin_metrics"
DEFAULT_SOURCE_VALUE = "coingecko"
PRICE_OHLC_METRIC = "price_ohlc"
VOLUME_METRIC = "volume"
TIMEFRAME = "day"
# Execution toggles
DRY_RUN_ENABLED = False  # When True, skip writes and log sample rows only.

# Request tuning
REQUEST_TIMEOUT = 30
RATE_LIMIT_MAX_RETRIES = 2
RATE_LIMIT_BACKOFF_SECONDS = 10

rds_conn = BaseHook.get_connection("rds_connection")
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)


def _upsert_do_nothing(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return 0
    insert_stmt = pg_insert(table.table).values(data)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
    result = conn.execute(do_nothing_stmt)
    return result.rowcount


def get_latest_timestamp_for_prefix(asset_symbol: str, metric_prefix: str, timeframe: str, source: str) -> Optional[datetime]:
    query = text(
        f"""
        SELECT MAX(timestamp) AS max_ts
        FROM {TABLE_NAME}
        WHERE asset_symbol = :asset
          AND timeframe = :timeframe
          AND source = :source
          AND metric LIKE :metric_like
        """
    )
    with rds_engine.begin() as conn:
        max_ts = conn.execute(
            query,
            {
                "asset": asset_symbol,
                "timeframe": timeframe,
                "source": source,
                "metric_like": f"{metric_prefix}%",
            },
        ).scalar()
    if max_ts is None:
        return None
    if isinstance(max_ts, datetime):
        return max_ts
    return pd.to_datetime(max_ts)


def load_api_key(key_name: str) -> str:
    """Load API key from Airflow Variables."""

    try:
        api_key = Variable.get(key_name)
        if not api_key:
            raise AirflowException(f"Variable {key_name} is empty.")
        return api_key
    except KeyError as exc:
        raise AirflowException(f"API key variable '{key_name}' not found") from exc


def pick_coingecko_days(latest_ts: Optional[datetime], now_utc: datetime) -> str:
    """Pick the smallest allowed days window that covers from latest_ts to now."""
    if latest_ts is None:
        return COINGECKO_DEFAULT_DAYS

    if latest_ts.tzinfo is None:
        latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
    else:
        latest_ts_utc = latest_ts.astimezone(timezone.utc)

    gap_days = (now_utc - latest_ts_utc).total_seconds() / 86400.0
    required_days = max(1, int(math.ceil(gap_days)))

    for bucket in COINGECKO_ALLOWED_DAYS:
        if required_days <= bucket:
            return bucket
    return COINGECKO_DEFAULT_DAYS


def make_coingecko_request(
    session: requests.Session,
    coin_id: str,
    vs_currency: str,
    days: str,
    api_key: str,
) -> List[List[Any]]:
    if not api_key:
        raise AirflowException("CoinGecko API key is missing or empty.")

    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/ohlc"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    headers = {"x-cg-pro-api-key": api_key}

    masked_key = f"{api_key[:4]}...{api_key[-2:]}" if api_key else "missing"
    logger.info("CoinGecko request start url=%s params=%s api_key_masked=%s", url, params, masked_key)

    response = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        response = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        if response.status_code == 429:
            sleep_seconds = RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            logger.warning(
                "Received 429 from CoinGecko for %s; sleeping %s seconds before retry (%s/%s)",
                coin_id,
                sleep_seconds,
                attempt + 1,
                RATE_LIMIT_MAX_RETRIES,
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code != 200:
            logger.error(
                "CoinGecko request failed status=%s url=%s params=%s body=%s",
                response.status_code,
                url,
                params,
                response.text,
            )
            raise AirflowException(f"CoinGecko request failed ({response.status_code}): {response.text}")

        break
    else:
        raise AirflowException(f"CoinGecko request failed after {RATE_LIMIT_MAX_RETRIES} retries")

    payload = response.json()
    if not isinstance(payload, list):
        raise AirflowException(f"Unexpected OHLC payload for {coin_id}: {payload}")
    return payload


def make_market_chart_request(
    session: requests.Session,
    coin_id: str,
    vs_currency: str,
    days: str,
    api_key: str,
) -> Dict[str, Any]:
    if not api_key:
        raise AirflowException("CoinGecko API key is missing or empty.")

    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    headers = {"x-cg-pro-api-key": api_key}

    masked_key = f"{api_key[:4]}...{api_key[-2:]}" if api_key else "missing"
    logger.info("CoinGecko market_chart start url=%s params=%s api_key_masked=%s", url, params, masked_key)

    response = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        response = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            sleep_seconds = RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            logger.warning(
                "Received 429 from CoinGecko for %s; sleeping %s seconds before retry (%s/%s)",
                coin_id,
                sleep_seconds,
                attempt + 1,
                RATE_LIMIT_MAX_RETRIES,
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code != 200:
            logger.error(
                "CoinGecko market_chart failed status=%s url=%s params=%s body=%s",
                response.status_code,
                url,
                params,
                response.text,
            )
            raise AirflowException(f"CoinGecko request failed ({response.status_code}): {response.text}")

        break
    else:
        raise AirflowException(f"CoinGecko request failed after {RATE_LIMIT_MAX_RETRIES} retries")

    payload = response.json()
    if not isinstance(payload, dict) or "total_volumes" not in payload:
        raise AirflowException(f"Unexpected market_chart payload for {coin_id}: {payload}")
    return payload


def normalize_volume_rows(
    raw_rows: List[List[Any]],
    asset_symbol: str,
    latest_ts: Optional[datetime],
) -> pd.DataFrame:
    if not raw_rows:
        return pd.DataFrame()

    # Drop the last row if it's incomplete
    trimmed = raw_rows[:-1] if len(raw_rows) > 0 else []

    latest_ts_utc: Optional[datetime] = None
    if latest_ts is not None:
        if latest_ts.tzinfo is None:
            latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        else:
            latest_ts_utc = latest_ts.astimezone(timezone.utc)

    records: List[Dict[str, Any]] = []
    for row in trimmed:
        # Expect [timestamp_ms, volume]
        if not isinstance(row, list) or len(row) < 2:
            continue
        ts = pd.to_datetime(row[0], unit="ms", utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        if latest_ts_utc is not None and ts <= latest_ts_utc:
            continue

        val = pd.to_numeric(row[1], errors="coerce")
        if pd.isna(val):
            continue

        ts_clean = ts.tz_convert("UTC").tz_localize(None)
        records.append(
            {
                "timestamp": ts_clean,
                "timeframe": TIMEFRAME,
                "asset_symbol": asset_symbol,
                "metric": VOLUME_METRIC,
                "source": DEFAULT_SOURCE_VALUE,
                "value": float(val),
            }
        )

    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)


def normalize_ohlc_rows(
    raw_rows: List[List[Any]],
    asset_symbol: str,
    latest_ts: Optional[datetime],
) -> pd.DataFrame:
    if not raw_rows:
        return pd.DataFrame()

    latest_ts_utc: Optional[datetime] = None
    if latest_ts is not None:
        if latest_ts.tzinfo is None:
            latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        else:
            latest_ts_utc = latest_ts.astimezone(timezone.utc)

    records: List[Dict[str, Any]] = []
    for row in raw_rows:
        # Expect [timestamp_ms, open, high, low, close]
        if not isinstance(row, list) or len(row) < 5:
            continue
        ts = pd.to_datetime(row[0], unit="ms", utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        if latest_ts_utc is not None and ts <= latest_ts_utc:
            continue

        ts_clean = ts.tz_convert("UTC").tz_localize(None)
        for suffix, val in zip(["open", "high", "low", "close"], row[1:5]):
            numeric_val = pd.to_numeric(val, errors="coerce")
            if pd.isna(numeric_val):
                continue
            records.append(
                {
                    "timestamp": ts_clean,
                    "timeframe": TIMEFRAME,
                    "asset_symbol": asset_symbol,
                    "metric": f"{PRICE_OHLC_METRIC}:{suffix}",
                    "source": DEFAULT_SOURCE_VALUE,
                    "value": float(numeric_val),
                }
            )

    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)


def fetch_and_store_coingecko_metrics(**context: Any) -> None:
    session = requests.Session()
    total_rows_written = 0
    failed_requests: List[str] = []
    ti = context.get("ti")
    api_key = load_api_key(COINGECKO_API_KEY_VAR)
    now_utc = datetime.now(timezone.utc)

    try:
        if DRY_RUN_ENABLED:
            logger.info("Dry-run enabled: will fetch and log samples without writing to DB.")
        for coin in COINGECKO_COINS:
            asset_symbol = coin["asset_symbol"]
            coin_id = coin["coingecko_id"]

            latest_ts = get_latest_timestamp_for_prefix(
                asset_symbol=asset_symbol,
                metric_prefix=f"{PRICE_OHLC_METRIC}:",
                timeframe=TIMEFRAME,
                source=DEFAULT_SOURCE_VALUE,
            )

            if latest_ts:
                if latest_ts.tzinfo is None:
                    latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
                else:
                    latest_ts_utc = latest_ts.astimezone(timezone.utc)

                if latest_ts_utc.date() == now_utc.date():
                    logger.info("Data up to date for %s (latest_ts=%s), skipping.", asset_symbol, latest_ts_utc.date())
                    continue

            days_param = pick_coingecko_days(latest_ts, now_utc)
            logger.info(
                "CoinGecko window for asset=%s latest_ts=%s days=%s",
                asset_symbol,
                latest_ts.isoformat() if latest_ts else "none",
                days_param,
            )

            try:
                raw_rows = make_coingecko_request(
                    session=session,
                    coin_id=coin_id,
                    vs_currency=COINGECKO_VS_CURRENCY,
                    days=str(days_param),
                    api_key=api_key,
                )
                if raw_rows:
                    logger.debug("Sample OHLC row for asset=%s: %s", asset_symbol, raw_rows[0])
            except AirflowException as exc:
                logger.error("Failed to fetch OHLC for asset %s (id=%s): %s", asset_symbol, coin_id, exc)
                failed_requests.append(f"{asset_symbol}:{coin_id} ({exc})")
                continue

            df = normalize_ohlc_rows(raw_rows, asset_symbol=asset_symbol, latest_ts=latest_ts)

            if df.empty:
                logger.info("No new OHLC rows for asset=%s", asset_symbol)
                continue

            if DRY_RUN_ENABLED:
                sample = df.head(5).to_dict(orient="records")
                logger.info(
                    "Dry-run: %s OHLC rows (showing up to 5) for asset=%s sample=%s",
                    len(df),
                    asset_symbol,
                    sample,
                )
                continue

            with rds_engine.begin() as conn:
                logger.info("Writing %s OHLC rows to %s for asset=%s", len(df), TABLE_NAME, asset_symbol)
                df.to_sql(
                    TABLE_NAME,
                    con=conn,
                    if_exists="append",
                    index=False,
                    method=_upsert_do_nothing,
                    chunksize=1000,
                )
            total_rows_written += len(df)
            time.sleep(0.2)  # gentle pacing

        if total_rows_written == 0:
            if failed_requests:
                logger.warning("No new CoinGecko data ingested. Failures=%s", failed_requests)
            else:
                logger.info("No new CoinGecko data to ingest.")
            if ti:
                ti.xcom_push(
                    key="coingecko_warnings",
                    value={"ingested_rows": 0, "failed_requests": failed_requests},
                )
            return

        logger.info("Inserted %s CoinGecko OHLC rows into %s", total_rows_written, TABLE_NAME)
        if failed_requests and ti:
            ti.xcom_push(
                key="coingecko_warnings",
                value={"ingested_rows": total_rows_written, "failed_requests": failed_requests},
            )
    finally:
        session.close()


def fetch_and_store_coingecko_volume(**context: Any) -> None:
    session = requests.Session()
    total_rows_written = 0
    failed_requests: List[str] = []
    ti = context.get("ti")
    api_key = load_api_key(COINGECKO_API_KEY_VAR)
    now_utc = datetime.now(timezone.utc)

    try:
        if DRY_RUN_ENABLED:
            logger.info("Dry-run enabled: will fetch and log samples without writing to DB.")
        for coin in COINGECKO_COINS:
            asset_symbol = coin["asset_symbol"]
            coin_id = coin["coingecko_id"]

            latest_ts = get_latest_timestamp_for_prefix(
                asset_symbol=asset_symbol,
                metric_prefix=VOLUME_METRIC,
                timeframe=TIMEFRAME,
                source=DEFAULT_SOURCE_VALUE,
            )

            if latest_ts:
                if latest_ts.tzinfo is None:
                    latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
                else:
                    latest_ts_utc = latest_ts.astimezone(timezone.utc)

                if latest_ts_utc.date() == now_utc.date():
                    logger.info("Volume up to date for %s (latest_ts=%s), skipping.", asset_symbol, latest_ts_utc.date())
                    continue

            days_param = pick_coingecko_days(latest_ts, now_utc)
            logger.info(
                "CoinGecko volume window for asset=%s latest_ts=%s days=%s",
                asset_symbol,
                latest_ts.isoformat() if latest_ts else "none",
                days_param,
            )

            try:
                payload = make_market_chart_request(
                    session=session,
                    coin_id=coin_id,
                    vs_currency=COINGECKO_VS_CURRENCY,
                    days=str(days_param),
                    api_key=api_key,
                )
            except AirflowException as exc:
                logger.error("Failed to fetch volume for asset %s (id=%s): %s", asset_symbol, coin_id, exc)
                failed_requests.append(f"{asset_symbol}:{coin_id} ({exc})")
                continue

            raw_rows = payload.get("total_volumes") or []
            df = normalize_volume_rows(raw_rows, asset_symbol=asset_symbol, latest_ts=latest_ts)

            if df.empty:
                logger.info("No new volume rows for asset=%s", asset_symbol)
                continue

            if DRY_RUN_ENABLED:
                sample = df.head(5).to_dict(orient="records")
                logger.info(
                    "Dry-run: %s volume rows (showing up to 5) for asset=%s sample=%s",
                    len(df),
                    asset_symbol,
                    sample,
                )
                continue

            with rds_engine.begin() as conn:
                logger.info("Writing %s volume rows to %s for asset=%s", len(df), TABLE_NAME, asset_symbol)
                df.to_sql(
                    TABLE_NAME,
                    con=conn,
                    if_exists="append",
                    index=False,
                    method=_upsert_do_nothing,
                    chunksize=1000,
                )
            total_rows_written += len(df)
            time.sleep(0.2)

        if total_rows_written == 0:
            if failed_requests:
                logger.warning("No new CoinGecko volume data ingested. Failures=%s", failed_requests)
            else:
                logger.info("No new CoinGecko volume data to ingest.")
            if ti:
                ti.xcom_push(
                    key="coingecko_volume_warnings",
                    value={"ingested_rows": 0, "failed_requests": failed_requests},
                )
            return

        logger.info("Inserted %s CoinGecko volume rows into %s", total_rows_written, TABLE_NAME)
        if failed_requests and ti:
            ti.xcom_push(
                key="coingecko_volume_warnings",
                value={"ingested_rows": total_rows_written, "failed_requests": failed_requests},
            )
    finally:
        session.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="coingecko_metrics_pipeline",
    default_args=default_args,
    description=get_dag_config("coingecko_metrics")["description"],
    schedule_interval=get_schedule_interval("coingecko_metrics"),
    start_date=get_start_date("coingecko_metrics"),
    catchup=False,
    max_active_runs=1,
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_coingecko_metrics",
        python_callable=fetch_and_store_coingecko_metrics,
        provide_context=True,
    )

    fetch_and_store_volume_task = PythonOperator(
        task_id="fetch_and_store_coingecko_volume",
        python_callable=fetch_and_store_coingecko_volume,
        provide_context=True,
    )

    fetch_and_store_task >> fetch_and_store_volume_task
