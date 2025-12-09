"""Airflow DAG for ingesting CryptoQuant market metrics."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from config.schedules import get_dag_config, get_schedule_interval, get_start_date


logger = logging.getLogger("cryptoquant_metrics")
logger.setLevel(logging.INFO)


CRYPTOQUANT_BASE_URL = "https://api.cryptoquant.com/v1"
CRYPTOQUANT_API_KEY_VAR = "api_key_cryptoquant"
DRY_RUN_ENABLED = False
CRYPTOQUANT_TABLE = "coin_metrics"
DRY_RUN_LIMIT = 5
REQUEST_TIMEOUT = 60
REQUEST_PAUSE_SECONDS = 0.2
DEFAULT_LIMIT = 10000
_CREDIT_HEADERS_LOGGED = False
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_BACKOFF_SECONDS = 5


# ERC20 tokens supported by CryptoQuant's ERC20 price-ohlcv endpoint.
# The API rejects the commented tokens with 400 "Invalid '<token>' token".
ERC20_SPOT_TOKENS = [
    # "sol",   # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    # "hype",  # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    "aave",
    # "pendle",  # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    # "aero",  # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    # "tao",   # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    # "ldo",   # Not in CryptoQuant supported ERC20 list; API returns invalid token.
    "uni",
]


MetricConfig = Dict[str, Any]


METRIC_CONFIGS: List[MetricConfig] = [
    {
        "metric": "price_close",
        "endpoint": "/btc/market-data/price-ohlcv",
        "value_key": "close",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/btc/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "btc",
                "request_params": {},
            }
        ],
    },
    {
        "metric": "price_close",
        "endpoint": "/eth/market-data/price-ohlcv",
        "value_key": "close",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/eth/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "eth",
                "request_params": {},
            }
        ],
    },
    {
        "metric": "price_close",
        "endpoint": "/alt/market-data/price-ohlcv",
        "value_key": "close",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/alt/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "sol",
                "request_params": {"token": "sol"},
            }
        ],
    },
    {
        "metric": "price_close",
        "endpoint": "/erc20/market-data/price-ohlcv",
        "value_key": "close",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/erc20/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": token,
                "request_params": {"token": token},
            }
            for token in ERC20_SPOT_TOKENS
        ],
    },
    {
        # ETH is not supported by the ERC20 endpoint; use native ETH price-ohlcv.
        "metric": "spot_volume",
        "endpoint": "/eth/market-data/price-ohlcv",
        "value_key": "volume",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/eth/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "eth",
                "request_params": {},
            }
        ],
    },
    {
        # Solana lives under the Alt list (not ERC20); use alt price-ohlcv.
        "metric": "spot_volume",
        "endpoint": "/alt/market-data/price-ohlcv",
        "value_key": "volume",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/alt/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "sol",
                "request_params": {"token": "sol"},
            }
        ],
    },
    {
        "metric": "spot_volume",
        "endpoint": "/erc20/market-data/price-ohlcv",
        "value_key": "volume",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/erc20/market-data/price-ohlcv",
        "static_params": {"window": "day", "format": "json", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": token,
                "request_params": {"token": token},
            }
            for token in ERC20_SPOT_TOKENS
        ],
    },
    {
        # Derivatives: open interest in USD (closest to futures OI/volume aggregate)
        "metric": "futures_open_interest",
        "endpoint": "/btc/market-data/open-interest",
        "value_key": "open_interest",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/btc/market-data/open-interest",
        "static_params": {"exchange": "all_exchange", "window": "day", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "btc",
                "request_params": {},
            }
        ],
    },
    {
        "metric": "futures_open_interest",
        "endpoint": "/eth/market-data/open-interest",
        "value_key": "open_interest",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/eth/market-data/open-interest",
        "static_params": {"exchange": "all_exchange", "window": "day", "limit": DEFAULT_LIMIT},
        "assets": [
            {
                "asset_symbol": "eth",
                "request_params": {},
            }
        ],
    },
    {
        "metric": "mvrv",
        "endpoint": "/btc/market-indicator/mvrv",
        "value_key": "mvrv",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/btc/market-indicator/mvrv",
        "static_params": {"window": "day", "limit": DEFAULT_LIMIT},
        "assets": [{"asset_symbol": "btc", "request_params": {}}],
    },
    {
        "metric": "sopr",
        "endpoint": "/btc/market-indicator/sopr",
        "value_key": "sopr",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/btc/market-indicator/sopr",
        "static_params": {"window": "day", "limit": DEFAULT_LIMIT},
        "assets": [{"asset_symbol": "btc", "request_params": {}}],
    },
    {
        "metric": "stablecoin_supply_ratio",
        "endpoint": "/btc/market-indicator/stablecoin-supply-ratio",
        "value_key": "stablecoin_supply_ratio",
        "timestamp_key": "date",
        "timeframe": "day",
        "source": "cryptoquant:/btc/market-indicator/stablecoin-supply-ratio",
        "static_params": {"window": "day", "limit": DEFAULT_LIMIT},
        "assets": [{"asset_symbol": "btc", "request_params": {}}],
    },
]


rds_conn = BaseHook.get_connection("rds_connection")
rds_engine = create_engine(
    f"postgresql://{rds_conn.login}:{rds_conn.password}@{rds_conn.host}:{rds_conn.port}/{rds_conn.schema}"
)


def load_api_key(key_name: str) -> str:
    """Load API key from Airflow Variables."""

    try:
        api_key = Variable.get(key_name)
        if not api_key:
            raise AirflowException(f"Variable {key_name} is empty.")
        return api_key
    except KeyError as exc:
        raise AirflowException(f"API key variable '{key_name}' not found") from exc


def make_cryptoquant_request(
    session: requests.Session,
    api_key: str,
    endpoint: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Call the CryptoQuant API and return the result payload."""

    global _CREDIT_HEADERS_LOGGED

    url = f"{CRYPTOQUANT_BASE_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}"}

    response = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        response = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        logger.debug("Request %s params=%s status=%s", endpoint, params, response.status_code)

        # Log credit/rate-limit headers once per run if available.
        if not _CREDIT_HEADERS_LOGGED:
            credit_headers = {
                k: v
                for k, v in response.headers.items()
                if k.lower()
                in {
                    "x-ratelimit-remaining",
                    "x-ratelimit-limit",
                    "x-ratelimit-reset",
                    "x-quota-remaining",
                    "x-quota-limit",
                }
            }
            if credit_headers:
                logger.info("CryptoQuant credit info: %s", credit_headers)
            _CREDIT_HEADERS_LOGGED = True

        if response.status_code == 429:
            reset_header = response.headers.get("x-ratelimit-reset")
            try:
                sleep_seconds = max(int(reset_header), 1) if reset_header is not None else None
            except ValueError:
                sleep_seconds = None
            if sleep_seconds is None:
                sleep_seconds = RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            logger.warning(
                "Received 429 from CryptoQuant for %s; sleeping %s seconds before retry (%s/%s)",
                endpoint,
                sleep_seconds,
                attempt + 1,
                RATE_LIMIT_MAX_RETRIES,
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code != 200:
            raise AirflowException(
                f"CryptoQuant request failed ({response.status_code}): {response.text}"
            )

        break
    else:
        raise AirflowException(f"CryptoQuant request failed after {RATE_LIMIT_MAX_RETRIES} retries")

    payload = response.json()
    status = payload.get("status", {})
    status_code = str(status.get("code", ""))
    if status and status_code not in {"0000", "0", "200"}:
        raise AirflowException(f"CryptoQuant error {status_code}: {status}")

    result = payload.get("result", [])
    if isinstance(result, dict):
        if "data" in result and isinstance(result["data"], list):
            result = result["data"]
        else:
            result = [result]
    if not isinstance(result, list):
        raise AirflowException(f"Unexpected result format for endpoint {endpoint}: {result}")
    return result


def is_dry_run() -> bool:
    """Return True if we should skip writes and minimize pulls."""

    return DRY_RUN_ENABLED


def get_latest_timestamp(asset_symbol: str, metric: str, timeframe: str) -> Optional[datetime]:
    """Fetch the most recent timestamp already stored for the given series."""

    query = text(
        f"""
        SELECT MAX(timestamp) AS max_ts
        FROM {CRYPTOQUANT_TABLE}
        WHERE asset_symbol = :asset
          AND metric = :metric
          AND timeframe = :timeframe
        """
    )
    with rds_engine.begin() as conn:
        max_ts = conn.execute(
            query, {"asset": asset_symbol, "metric": metric, "timeframe": timeframe}
        ).scalar()
    if max_ts is None:
        return None
    if isinstance(max_ts, datetime):
        return max_ts
    return pd.to_datetime(max_ts)


def build_from_parameter(latest_ts: Optional[datetime], timeframe: str) -> Optional[str]:
    """Convert the latest timestamp stored into the from= parameter format."""

    if latest_ts is None:
        return None

    increments = {
        "day": timedelta(days=1),
        "hour": timedelta(hours=1),
        "min": timedelta(minutes=1),
    }
    delta = increments.get(timeframe)
    if not delta:
        return None

    next_point = latest_ts + delta
    if timeframe == "day":
        return next_point.strftime("%Y%m%d")
    return next_point.strftime("%Y%m%dT%H%M%S")


def normalize_dataframe(
    raw_rows: List[Dict[str, Any]],
    value_key: str,
    timestamp_key: str,
    asset_symbol: str,
    metric: str,
    timeframe: str,
    source: str,
    latest_ts: Optional[datetime],
) -> Tuple[pd.DataFrame, Optional[str]]:
    """Convert raw API rows into the canonical table schema."""

    if not raw_rows:
        return pd.DataFrame(), None

    df = pd.DataFrame(raw_rows)
    if df.empty or timestamp_key not in df.columns or value_key not in df.columns:
        logger.warning(
            "Missing expected columns for metric %s (asset=%s). Columns=%s sample=%s",
            metric,
            asset_symbol,
            list(df.columns),
            raw_rows[0] if raw_rows else {},
        )
        # Push the raw sample row back to Airflow logs/XCom if present to help mapping.
        return pd.DataFrame(), "missing_columns"

    df["timestamp"] = pd.to_datetime(df[timestamp_key], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df[value_key], errors="coerce")
    df = df.dropna(subset=["timestamp", "value"])

    if latest_ts is not None:
        if latest_ts.tzinfo is None:
            latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        else:
            latest_ts_utc = latest_ts.astimezone(timezone.utc)
        df = df[df["timestamp"] > latest_ts_utc]

    if df.empty:
        return df, None

    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["asset_symbol"] = asset_symbol
    df["metric"] = metric
    df["timeframe"] = timeframe
    df["source"] = source
    df = df[["timestamp", "timeframe", "asset_symbol", "metric", "source", "value"]]
    df = df.drop_duplicates(subset=["timestamp", "timeframe", "asset_symbol", "metric"])
    return df, None


def fetch_and_store_cryptoquant_metrics(**context: Any) -> None:
    """Main task that fetches CryptoQuant metrics and loads them into Postgres."""

    api_key = load_api_key(CRYPTOQUANT_API_KEY_VAR)
    dry_run = is_dry_run()
    session = requests.Session()
    frames: List[pd.DataFrame] = []
    failed_requests: List[str] = []
    missing_columns: List[str] = []
    ti = context.get("ti")

    try:
        for config in METRIC_CONFIGS:
            metric = config["metric"]
            endpoint = config["endpoint"]
            timeframe = config["timeframe"]
            static_params = dict(config.get("static_params", {}))
            value_key = config["value_key"]
            timestamp_key = config["timestamp_key"]
            source = config["source"]

            for asset in config["assets"]:
                asset_symbol = asset["asset_symbol"]
                params = {**static_params, **asset.get("request_params", {})}
                params.setdefault("limit", DEFAULT_LIMIT)
                if dry_run:
                    params["limit"] = min(params["limit"], DRY_RUN_LIMIT)

                latest_ts = None if dry_run else get_latest_timestamp(
                    asset_symbol, metric, timeframe
                )
                if not dry_run:
                    from_param = build_from_parameter(latest_ts, timeframe)
                    if from_param:
                        params["from"] = from_param

                try:
                    raw_rows = make_cryptoquant_request(session, api_key, endpoint, params)
                    if raw_rows:
                        logger.debug(
                            "Sample raw row for metric=%s asset=%s: %s",
                            metric,
                            asset_symbol,
                            raw_rows[0],
                        )
                except AirflowException as exc:
                    logger.error(
                        "Failed to fetch metric %s for asset %s: %s", metric, asset_symbol, exc
                    )
                    failed_requests.append(f"{metric}:{asset_symbol} ({exc})")
                    continue

                df, issue = normalize_dataframe(
                    raw_rows,
                    value_key=value_key,
                    timestamp_key=timestamp_key,
                    asset_symbol=asset_symbol,
                    metric=metric,
                    timeframe=timeframe,
                    source=source,
                    latest_ts=latest_ts,
                )
                if issue == "missing_columns":
                    missing_columns.append(f"{metric}:{asset_symbol}")

                if df.empty:
                    logger.info(
                        "No new rows for metric=%s asset=%s timeframe=%s",
                        metric,
                        asset_symbol,
                        timeframe,
                    )
                else:
                    frames.append(df)
                    logger.info(
                        "Prepared %s new rows for metric=%s asset=%s timeframe=%s (dry_run=%s)",
                        len(df),
                        metric,
                        asset_symbol,
                        timeframe,
                        dry_run,
                    )

                time.sleep(REQUEST_PAUSE_SECONDS)

        if not frames:
            if failed_requests or missing_columns:
                logger.warning(
                    "No new CryptoQuant data ingested. Failures=%s missing_columns=%s",
                    failed_requests if failed_requests else "none",
                    missing_columns if missing_columns else "none",
                )
            else:
                logger.info("No new CryptoQuant data to ingest.")
            if ti:
                ti.xcom_push(
                    key="cryptoquant_warnings",
                    value={
                        "ingested_rows": 0,
                        "failed_requests": failed_requests,
                        "missing_columns": missing_columns,
                        "dry_run": dry_run,
                    },
                )
            return

        final_df = pd.concat(frames, ignore_index=True)
        final_df.sort_values("timestamp", inplace=True)

        if dry_run:
            logger.info(
                "Dry-run enabled; skipping write of %s rows. Sample:\n%s",
                len(final_df),
                final_df.head().to_string(index=False) if not final_df.empty else "[]",
            )
            if ti:
                ti.xcom_push(
                    key="cryptoquant_warnings",
                    value={
                        "ingested_rows": len(final_df),
                        "failed_requests": failed_requests,
                        "missing_columns": missing_columns,
                        "dry_run": True,
                    },
                )
            return

        with rds_engine.begin() as conn:
            final_df.to_sql(
                CRYPTOQUANT_TABLE,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        logger.info("Inserted %s rows into %s", len(final_df), CRYPTOQUANT_TABLE)
        if failed_requests or missing_columns:
            logger.warning(
                "Completed with partial issues. Failed requests=%s; missing_columns=%s",
                failed_requests if failed_requests else "none",
                missing_columns if missing_columns else "none",
            )
        if ti:
            ti.xcom_push(
                key="cryptoquant_warnings",
                value={
                    "ingested_rows": len(final_df),
                    "failed_requests": failed_requests,
                    "missing_columns": missing_columns,
                    "dry_run": False,
                },
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
    dag_id="cryptoquant_metrics_pipeline",
    default_args=default_args,
    description=get_dag_config("cryptoquant_metrics")["description"],
    schedule_interval=get_schedule_interval("cryptoquant_metrics"),
    start_date=get_start_date("cryptoquant_metrics"),
    catchup=False,
    max_active_runs=1,
) as dag:

    ingest_cryptoquant_metrics = PythonOperator(
        task_id="ingest_cryptoquant_metrics",
        python_callable=fetch_and_store_cryptoquant_metrics,
    )

