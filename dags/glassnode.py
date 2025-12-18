"""Airflow DAG for ingesting Glassnode market metrics."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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


logger = logging.getLogger("glassnode_metrics")
logger.setLevel(logging.INFO)


# API + credentials
GLASSNODE_BASE_URL = "https://api.glassnode.com"  # Base host for all Glassnode API calls.
GLASSNODE_API_KEY_VAR = "api_key_glassnode"  # Airflow Variable name holding the Glassnode API key.

# Execution toggles
RESET_IGNORE_HISTORY = False  # When True, ignore latest timestamp and fetch full history (no date truncation).
DRY_RUN_ENABLED = False  # Skip writes and minimize pull scope when True.
DRY_RUN_LOOKBACK_DAYS = 3  # Window to pull when no history exists during dry-run.

# Scope filters (empty -> no filtering)
ASSET_FILTER: List[str] = []  # Assets to ingest across all runs.
METRIC_FILTER: List[str] = ["us_spot_etf_balances_all", "us_spot_etf_flows_all", "us_spot_etf_flows_net"]  # Metrics to ingest across all runs.

# Table/target
TABLE_NAME = "coin_metrics"  # Target Postgres table for all Glassnode ingestions.
DEFAULT_SOURCE_VALUE = "glassnode"  # Source label stored in the DB.

# Request tuning
REQUEST_TIMEOUT = 60  # HTTP request timeout in seconds.
REQUEST_PAUSE_SECONDS = 0.2  # Pause between API calls to avoid rate limits.
DEFAULT_LIMIT = 10000  # Generic row limit placeholder (unused for Glassnode basic API).
RATE_LIMIT_MAX_RETRIES = 2  # Number of retries on 429 responses.
RATE_LIMIT_BACKOFF_SECONDS = 60  # Backoff base seconds between retries for 429.

# Use the full token list (including previously commented tokens) to probe coverage.
GLASSNODE_ASSETS = ["btc", "eth", "sol", "hype", "aave", "pendle", "ethfi", "ldo", "tao", "uni", "aero", "zec"]

# Per-endpoint allowlists based on Glassnode support to avoid 400s.
TOP_TWO_ASSETS = ["btc", "eth"]
BTC_ONLY = ["btc"]
DERIVATIVE_ASSETS = ["btc", "eth", "bnb", "doge", "sol", "ton", "trx", "xrp", "usdt", "usdc", "zec"]
OPTIONS_ASSETS = ["btc", "eth", "sol", "xrp", "zec"]

SCALAR_ASSET_ALLOWLISTS: Dict[str, List[str]] = {
    # Broadly supported spot/derivatives metrics
    "spot_volume": GLASSNODE_ASSETS,
    "futures_volume_all": GLASSNODE_ASSETS,
    "futures_volume_perpetual": GLASSNODE_ASSETS,
    "futures_open_interest": DERIVATIVE_ASSETS,
    "futures_open_interest_cash_margin": DERIVATIVE_ASSETS,
    "futures_open_interest_crypto_margin": DERIVATIVE_ASSETS,
    "funding_rate_perpetual": DERIVATIVE_ASSETS,
    "funding_rate_perpetual_all": DERIVATIVE_ASSETS,
    "futures_open_interest_latest": DERIVATIVE_ASSETS,
    "futures_open_interest_crypto_margin_relative": DERIVATIVE_ASSETS,
    "futures_open_interest_cash_margin_perpetual_sum": DERIVATIVE_ASSETS,
    "futures_open_interest_sum_all": DERIVATIVE_ASSETS,
    "futures_open_interest_perpetual_sum_all": DERIVATIVE_ASSETS,
    "futures_cme_open_interest_sum": TOP_TWO_ASSETS,
    "futures_cme_open_interest_cash_margin_sum": TOP_TWO_ASSETS,
    "futures_cme_open_interest_crypto_margin_sum": TOP_TWO_ASSETS,
    "options_open_interest_put_call_ratio": OPTIONS_ASSETS,
    "options_open_interest_sum": OPTIONS_ASSETS,
    "options_25delta_skew_1w": OPTIONS_ASSETS,
    "options_25delta_skew_1m": OPTIONS_ASSETS,
    "options_atm_implied_volatility_1w": OPTIONS_ASSETS,
    "options_atm_implied_volatility_1m": OPTIONS_ASSETS,
    "mvrv_z_score": ["btc", "eth", "sol", "aave", "pendle", "ethfi", "ldo", "uni"],
    "mvrv": TOP_TWO_ASSETS,
    "mvrv_median": TOP_TWO_ASSETS,
    "mvrv_less_155": TOP_TWO_ASSETS,
    "mvrv_more_155": TOP_TWO_ASSETS,
    "sopr": ["btc", "eth", "sol", "aave", "pendle", "ethfi", "ldo", "uni"],
    "sopr_adjusted": ["btc"],
    "net_realized_profit_loss": TOP_TWO_ASSETS,
    "net_unrealized_profit_loss": TOP_TWO_ASSETS,
    "balance_exchanges": TOP_TWO_ASSETS,
    "balance_exchanges_relative": TOP_TWO_ASSETS,
    "exchange_net_position_change": TOP_TWO_ASSETS,
    "percent_addresses_in_profit": ["btc", "eth", "sol", "aave", "pendle", "ethfi", "ldo", "uni"],
    "supply_by_date_bands": BTC_ONLY,
    "supply_by_txout_type": BTC_ONLY,
    "transfers_volume_exchanges_to_whales_sum": TOP_TWO_ASSETS,
    "transfers_volume_from_miners_sum": BTC_ONLY,
    "balance_miners_change": BTC_ONLY,
    # Institutions - US Spot ETF
    "us_spot_etf_balances_all": ["btc", "eth"],
    "us_spot_etf_flows_all": ["btc", "eth"],
    "us_spot_etf_flows_net": ["btc", "eth"],
    
    # Indicator signals (catalog-backed)
    "altcoin_cycle_signal": ["btc"],
    "btc_risk_signal": ["btc"],
    "btc_sharpe_signal": ["btc"],
    "btc_bss_indicator_1": ["btc"],
    "btc_bss_indicator_2": ["btc"],
    "btc_bss_indicator_3": ["btc"],
    "btc_bss_indicator_4": ["btc"],
    "btc_bss_short": ["btc"],
    "btc_bss_goldilocks_short": ["btc"],
    "btc_bss_goldilocks": ["btc"],
    "ecosystem_momentum_index": ["atom", "bnb", "dot", "matic", "pol", "sol"],
    "btc_bss_v2": ["btc"],
}

# Breakdown endpoints are restricted to a narrower allowlist (per Glassnode docs).
BREAKDOWN_ASSETS = ["btc", "eth", "aave"]


MetricConfig = Dict[str, Any]


def _to_asset_param(symbol: str) -> str:
    return symbol.upper()


def _build_asset_params(symbols: List[str]) -> List[Dict[str, Any]]:
    return [{"asset_symbol": token, "request_params": {"a": _to_asset_param(token)}} for token in symbols]


ASSET_PARAMS: List[Dict[str, Any]] = _build_asset_params(GLASSNODE_ASSETS)

BREAKDOWN_ASSET_PARAMS: List[Dict[str, Any]] = _build_asset_params(BREAKDOWN_ASSETS)


def _make_scalar_metric(
    metric: str,
    endpoint: str,
    currency: bool = True,
    extra_params: Optional[Dict[str, Any]] = None,
    allowed_assets: Optional[List[str]] = None,
) -> MetricConfig:
    base_params: Dict[str, Any] = {"interval": "24h", "format": "json"}
    if currency:
        base_params["currency"] = "USD"
    if extra_params:
        base_params.update(extra_params)
    assets = _build_asset_params(allowed_assets) if allowed_assets is not None else ASSET_PARAMS
    return {
        "metric": metric,
        "endpoint": endpoint,
        "value_key": "v",
        "timestamp_key": "t",
        "timeframe": "day",
        "source": DEFAULT_SOURCE_VALUE,
        "static_params": base_params,
        "assets": assets,
    }


def _make_signal_metric(
    metric: str,
    endpoint: str,
    interval: str,
    timeframe: str,
    allowed_assets: List[str],
) -> MetricConfig:
    cfg = _make_scalar_metric(
        metric,
        endpoint,
        currency=False,
        extra_params={"interval": interval},
        allowed_assets=allowed_assets,
    )
    cfg["timeframe"] = timeframe
    # Signals follow the short param naming (i/s/u), matching OHLC behavior
    cfg["interval_param"] = "i"
    cfg["since_param"] = "s"
    cfg["until_param"] = "u"
    return cfg


def _make_breakdown_metric(metric: str, endpoint: str, allowed_assets: Optional[List[str]] = None) -> MetricConfig:
    assets = _build_asset_params(allowed_assets) if allowed_assets is not None else BREAKDOWN_ASSET_PARAMS
    return {
        "metric": metric,
        "endpoint": endpoint,
        "value_key": "o",
        "timestamp_key": "t",
        "timeframe": "day",
        "source": DEFAULT_SOURCE_VALUE,
        "static_params": {"interval": "24h", "format": "json"},
        "assets": assets,
    }


PRICE_OHLC_MAP = {"o": "open", "h": "high", "l": "low", "c": "close"}


def _make_price_ohlc(interval: str, timeframe: str) -> MetricConfig:
    return {
        "metric": "price_ohlc",
        "endpoint": "/v1/metrics/market/price_usd_ohlc",
        "value_key": None,
        "multi_value_map": PRICE_OHLC_MAP,
        "timestamp_key": "t",
        "timeframe": timeframe,
        "source": DEFAULT_SOURCE_VALUE,
        "static_params": {"interval": interval, "format": "json", "currency": "USD"},
        "interval_param": "i",
        "since_param": "s",
        "until_param": "u",
        "assets": ASSET_PARAMS,
    }


METRIC_CONFIGS: List[MetricConfig] = [
    # Indicator signals (catalog-backed)
    _make_signal_metric(
        "altcoin_cycle_signal",
        "/v1/metrics/signals/altcoin_index",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["altcoin_cycle_signal"],
    ),
    _make_signal_metric(
        "btc_risk_signal",
        "/v1/metrics/signals/btc_risk_index",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_risk_signal"],
    ),
    _make_signal_metric(
        "btc_risk_signal",
        "/v1/metrics/signals/btc_risk_index",
        interval="1h",
        timeframe="hour",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_risk_signal"],
    ),
    _make_signal_metric(
        "btc_sharpe_signal",
        "/v1/metrics/signals/btc_sharpe_signal",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_sharpe_signal"],
    ),
    _make_signal_metric(
        "btc_bss_indicator_1",
        "/v1/metrics/signals/btc_bss_indicator_1",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_indicator_1"],
    ),
    _make_signal_metric(
        "btc_bss_indicator_2",
        "/v1/metrics/signals/btc_bss_indicator_2",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_indicator_2"],
    ),
    _make_signal_metric(
        "btc_bss_indicator_3",
        "/v1/metrics/signals/btc_bss_indicator_3",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_indicator_3"],
    ),
    _make_signal_metric(
        "btc_bss_indicator_4",
        "/v1/metrics/signals/btc_bss_indicator_4",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_indicator_4"],
    ),
    _make_signal_metric(
        "btc_bss_short",
        "/v1/metrics/signals/btc_bss_short",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_short"],
    ),
    _make_signal_metric(
        "btc_bss_short",
        "/v1/metrics/signals/btc_bss_short",
        interval="1h",
        timeframe="hour",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_short"],
    ),
    _make_signal_metric(
        "btc_bss_goldilocks_short",
        "/v1/metrics/signals/btc_bss_goldilocks_short",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_goldilocks_short"],
    ),
    _make_signal_metric(
        "btc_bss_goldilocks_short",
        "/v1/metrics/signals/btc_bss_goldilocks_short",
        interval="1h",
        timeframe="hour",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_goldilocks_short"],
    ),
    _make_signal_metric(
        "btc_bss_goldilocks",
        "/v1/metrics/signals/btc_bss_goldilocks",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_goldilocks"],
    ),
    _make_signal_metric(
        "ecosystem_momentum_index",
        "/v1/metrics/signals/ecosystem_momentum_index",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["ecosystem_momentum_index"],
    ),
    _make_signal_metric(
        "ecosystem_momentum_index",
        "/v1/metrics/signals/ecosystem_momentum_index",
        interval="1h",
        timeframe="hour",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["ecosystem_momentum_index"],
    ),
    _make_signal_metric(
        "btc_bss_v2",
        "/v1/metrics/signals/btc_bss_v2",
        interval="24h",
        timeframe="day",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_v2"],
    ),
    _make_signal_metric(
        "btc_bss_v2",
        "/v1/metrics/signals/btc_bss_v2",
        interval="1h",
        timeframe="hour",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["btc_bss_v2"],
    ),
    # Institutions - US Spot ETFs
    _make_breakdown_metric(
        "us_spot_etf_balances_all",
        "/v1/metrics/institutions/us_spot_etf_balances_all",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["us_spot_etf_balances_all"],
    ),
    _make_breakdown_metric(
        "us_spot_etf_flows_all",
        "/v1/metrics/institutions/us_spot_etf_flows_all",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["us_spot_etf_flows_all"],
    ),
    _make_scalar_metric(
        "us_spot_etf_flows_net",
        "/v1/metrics/institutions/us_spot_etf_flows_net",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["us_spot_etf_flows_net"],
    ),
    _make_price_ohlc("24h", "day"),
    # _make_price_ohlc("1h", "hour"),
    # _make_price_ohlc("10m", "10m"),
    _make_scalar_metric("spot_volume", "/v1/metrics/market/spot_volume_sum_intraday"),
    _make_scalar_metric(
        "futures_volume_all", "/v1/metrics/derivatives/futures_volume_sum", allowed_assets=DERIVATIVE_ASSETS
    ),
    _make_scalar_metric(
        "futures_volume_perpetual",
        "/v1/metrics/derivatives/futures_volume_perpetual_sum",
        allowed_assets=DERIVATIVE_ASSETS,
    ),
    _make_scalar_metric(
        "futures_open_interest",
        "/v1/metrics/derivatives/futures_open_interest_sum",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest"],
    ),
    _make_scalar_metric(
        "futures_open_interest_cash_margin",
        "/v1/metrics/derivatives/futures_open_interest_cash_margin_sum",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_cash_margin"],
    ),
    _make_scalar_metric(
        "futures_open_interest_crypto_margin",
        "/v1/metrics/derivatives/futures_open_interest_crypto_margin_sum",
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_crypto_margin"],
    ),
    _make_scalar_metric(
        "funding_rate_perpetual",
        "/v1/metrics/derivatives/futures_funding_rate_perpetual",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["funding_rate_perpetual"],
    ),
    _make_scalar_metric(
        "mvrv_z_score",
        "/v1/metrics/market/mvrv_z_score",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["mvrv_z_score"],
    ),
    _make_scalar_metric(
        "mvrv",
        "/v1/metrics/market/mvrv",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["mvrv"],
    ),
    _make_scalar_metric(
        "mvrv_median",
        "/v1/metrics/market/mvrv_median",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["mvrv_median"],
    ),
    _make_scalar_metric(
        "mvrv_less_155",
        "/v1/metrics/market/mvrv_less_155",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["mvrv_less_155"],
    ),
    _make_scalar_metric(
        "mvrv_more_155",
        "/v1/metrics/market/mvrv_more_155",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["mvrv_more_155"],
    ),
    _make_scalar_metric(
        "sopr",
        "/v1/metrics/indicators/sopr",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["sopr"],
    ),
    _make_scalar_metric(
        "sopr_adjusted",
        "/v1/metrics/indicators/sopr_adjusted",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["sopr_adjusted"],
    ),
    _make_scalar_metric(
        "percent_addresses_in_profit",
        "/v1/metrics/addresses/profit_relative",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["percent_addresses_in_profit"],
    ),
    _make_scalar_metric(
        "net_realized_profit_loss",
        "/v1/metrics/indicators/net_realized_profit_loss",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["net_realized_profit_loss"],
    ),
    _make_scalar_metric(
        "net_unrealized_profit_loss",
        "/v1/metrics/indicators/net_unrealized_profit_loss",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["net_unrealized_profit_loss"],
    ),
    _make_scalar_metric(
        "balance_exchanges",
        "/v1/metrics/distribution/balance_exchanges",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["balance_exchanges"],
    ),
    _make_scalar_metric(
        "balance_exchanges_relative",
        "/v1/metrics/distribution/balance_exchanges_relative",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["balance_exchanges_relative"],
    ),
    _make_scalar_metric(
        "exchange_net_position_change",
        "/v1/metrics/distribution/exchange_net_position_change",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["exchange_net_position_change"],
    ),
    _make_scalar_metric(
        "supply_by_date_bands",
        "/v1/metrics/supply/supply_by_date_bands",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["supply_by_date_bands"],
    ),
    _make_scalar_metric(
        "supply_by_txout_type",
        "/v1/metrics/supply/supply_by_txout_type",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["supply_by_txout_type"],
    ),
    _make_scalar_metric(
        "futures_funding_rate_perpetual_all",
        "/v1/metrics/derivatives/futures_funding_rate_perpetual_all",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["funding_rate_perpetual_all"],
    ),
    _make_scalar_metric(
        "futures_open_interest_latest",
        "/v1/metrics/derivatives/futures_open_interest_latest",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_latest"],
    ),
    _make_scalar_metric(
        "futures_open_interest_crypto_margin_relative",
        "/v1/metrics/derivatives/futures_open_interest_crypto_margin_relative",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_crypto_margin_relative"],
    ),
    _make_scalar_metric(
        "futures_open_interest_cash_margin_perpetual_sum",
        "/v1/metrics/derivatives/futures_open_interest_cash_margin_perpetual_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_cash_margin_perpetual_sum"],
    ),
    _make_scalar_metric(
        "futures_open_interest_sum_all",
        "/v1/metrics/derivatives/futures_open_interest_sum_all",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_sum_all"],
    ),
    _make_scalar_metric(
        "futures_open_interest_perpetual_sum_all",
        "/v1/metrics/derivatives/futures_open_interest_perpetual_sum_all",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_open_interest_perpetual_sum_all"],
    ),
    _make_scalar_metric(
        "futures_cme_open_interest_sum",
        "/v1/metrics/derivatives/futures_cme_open_interest_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_cme_open_interest_sum"],
    ),
    _make_scalar_metric(
        "futures_cme_open_interest_cash_margin_sum",
        "/v1/metrics/derivatives/futures_cme_open_interest_cash_margin_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_cme_open_interest_cash_margin_sum"],
    ),
    _make_scalar_metric(
        "futures_cme_open_interest_crypto_margin_sum",
        "/v1/metrics/derivatives/futures_cme_open_interest_crypto_margin_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["futures_cme_open_interest_crypto_margin_sum"],
    ),
    _make_scalar_metric(
        "options_open_interest_put_call_ratio",
        "/v1/metrics/derivatives/options_open_interest_put_call_ratio",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_open_interest_put_call_ratio"],
    ),
    _make_scalar_metric(
        "options_open_interest_sum",
        "/v1/metrics/derivatives/options_open_interest_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_open_interest_sum"],
    ),
    _make_scalar_metric(
        "options_25delta_skew_1w",
        "/v1/metrics/derivatives/options_25delta_skew_1w",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_25delta_skew_1w"],
    ),
    _make_scalar_metric(
        "options_25delta_skew_1m",
        "/v1/metrics/derivatives/options_25delta_skew_1m",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_25delta_skew_1m"],
    ),
    _make_scalar_metric(
        "options_atm_implied_volatility_1w",
        "/v1/metrics/derivatives/options_atm_implied_volatility_1w",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_atm_implied_volatility_1w"],
    ),
    _make_scalar_metric(
        "options_atm_implied_volatility_1m",
        "/v1/metrics/derivatives/options_atm_implied_volatility_1m",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["options_atm_implied_volatility_1m"],
    ),
    _make_scalar_metric(
        "transfers_volume_exchanges_to_whales_sum",
        "/v1/metrics/transactions/transfers_volume_exchanges_to_whales_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["transfers_volume_exchanges_to_whales_sum"],
    ),
    _make_scalar_metric(
        "transfers_volume_from_miners_sum",
        "/v1/metrics/transactions/transfers_volume_from_miners_sum",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["transfers_volume_from_miners_sum"],
    ),
    _make_scalar_metric(
        "balance_miners_change",
        "/v1/metrics/distribution/balance_miners_change",
        currency=False,
        allowed_assets=SCALAR_ASSET_ALLOWLISTS["balance_miners_change"],
    ),
    # Breakdown endpoints (bucketed; flattened into metric:bucket)
    _make_breakdown_metric("sopr_by_age", "/v1/metrics/breakdowns/sopr_by_age"),
    _make_breakdown_metric("sopr_by_lth_sth", "/v1/metrics/breakdowns/sopr_by_lth_sth"),
    _make_breakdown_metric("sopr_by_pnl", "/v1/metrics/breakdowns/sopr_by_pnl"),
    _make_breakdown_metric("sopr_by_wallet_size", "/v1/metrics/breakdowns/sopr_by_wallet_size"),
    _make_breakdown_metric("mvrv_by_age", "/v1/metrics/breakdowns/mvrv_by_age", allowed_assets=TOP_TWO_ASSETS),
    _make_breakdown_metric("mvrv_by_pnl", "/v1/metrics/breakdowns/mvrv_by_pnl", allowed_assets=TOP_TWO_ASSETS),
    _make_breakdown_metric(
        "mvrv_by_wallet_size", "/v1/metrics/breakdowns/mvrv_by_wallet_size", allowed_assets=TOP_TWO_ASSETS
    ),
    _make_breakdown_metric("supply_by_pnl_relative", "/v1/metrics/breakdowns/supply_by_pnl_relative"),
    _make_breakdown_metric("supply_by_age", "/v1/metrics/breakdowns/supply_by_age"),
    _make_breakdown_metric(
        "supply_by_investor_behavior", "/v1/metrics/breakdowns/supply_by_investor_behavior"
    ),
    _make_breakdown_metric("supply_by_pnl", "/v1/metrics/breakdowns/supply_by_pnl"),
    _make_breakdown_metric("supply_by_wallet_size", "/v1/metrics/breakdowns/supply_by_wallet_size"),
]


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


def load_api_key(key_name: str) -> str:
    """Load API key from Airflow Variables."""

    try:
        api_key = Variable.get(key_name)
        if not api_key:
            raise AirflowException(f"Variable {key_name} is empty.")
        return api_key
    except KeyError as exc:
        raise AirflowException(f"API key variable '{key_name}' not found") from exc


def make_glassnode_request(
    session: requests.Session,
    api_key: str,
    endpoint: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Call the Glassnode API and return the result payload."""

    url = f"{GLASSNODE_BASE_URL}{endpoint}"
    params_with_key = {**params, "api_key": api_key}

    response = None
    for attempt in range(RATE_LIMIT_MAX_RETRIES + 1):
        response = session.get(url, params=params_with_key, timeout=REQUEST_TIMEOUT)
        logger.debug("Glassnode request %s params=%s status=%s", endpoint, params_with_key, response.status_code)

        if response.status_code == 429:
            sleep_seconds = RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
            logger.warning(
                "Received 429 from Glassnode for %s; sleeping %s seconds before retry (%s/%s)",
                endpoint,
                sleep_seconds,
                attempt + 1,
                RATE_LIMIT_MAX_RETRIES,
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code != 200:
            raise AirflowException(
                f"Glassnode request failed ({response.status_code}): {response.text}"
            )

        break
    else:
        raise AirflowException(f"Glassnode request failed after {RATE_LIMIT_MAX_RETRIES} retries")

    payload = response.json()
    if not isinstance(payload, list):
        raise AirflowException(f"Unexpected result format for endpoint {endpoint}: {payload}")
    return payload


def is_dry_run() -> bool:
    return DRY_RUN_ENABLED


def get_latest_timestamp(asset_symbol: str, metric: str, timeframe: str, source: str) -> Optional[datetime]:
    query = text(
        f"""
        SELECT MAX(timestamp) AS max_ts
        FROM {TABLE_NAME}
        WHERE asset_symbol = :asset
          AND metric = :metric
          AND timeframe = :timeframe
          AND source = :source
        """
    )
    with rds_engine.begin() as conn:
        max_ts = conn.execute(
            query,
            {
                "asset": asset_symbol,
                "metric": metric,
                "timeframe": timeframe,
                "source": source,
            },
        ).scalar()
    if max_ts is None:
        return None
    if isinstance(max_ts, datetime):
        return max_ts
    return pd.to_datetime(max_ts)


def build_since_parameter(latest_ts: Optional[datetime], timeframe: str) -> Optional[int]:
    if latest_ts is None:
        return None

    increments = {
        "day": timedelta(days=1),
        "hour": timedelta(hours=1),
        "min": timedelta(minutes=1),
        "10m": timedelta(minutes=10),
    }
    delta = increments.get(timeframe)
    if not delta:
        return None

    next_point = latest_ts + delta
    return int(next_point.replace(tzinfo=timezone.utc).timestamp())


def normalize_dataframe(
    raw_rows: List[Dict[str, Any]],
    value_key: str,
    timestamp_key: str,
    asset_symbol: str,
    metric: str,
    timeframe: str,
    source: str,
    latest_ts: Optional[datetime],
    multi_value_map: Optional[Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, Optional[str]]:
    if not raw_rows:
        return pd.DataFrame(), None

    latest_ts_utc: Optional[datetime] = None
    if latest_ts is not None:
        if latest_ts.tzinfo is None:
            latest_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        else:
            latest_ts_utc = latest_ts.astimezone(timezone.utc)

    records: List[Dict[str, Any]] = []

    def _append_record(metric_name: str, ts: pd.Timestamp, val: Any) -> None:
        # Some endpoints may wrap the numeric in a dict (e.g., {"v": 123}).
        if isinstance(val, dict):
            val = val.get("v")
        try:
            numeric_val = pd.to_numeric(val, errors="coerce")
        except Exception:
            numeric_val = pd.NA
        if pd.isna(numeric_val):
            return
        ts_clean = ts.tz_convert("UTC").tz_localize(None)
        records.append(
            {
                "timestamp": ts_clean,
                "timeframe": timeframe,
                "asset_symbol": asset_symbol,
                "metric": metric_name,
                "source": source,
                "value": float(numeric_val),
            }
        )

    for row in raw_rows:
        if timestamp_key not in row:
            continue
        ts = pd.to_datetime(row.get(timestamp_key), unit="s", utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        if latest_ts_utc is not None and ts <= latest_ts_utc:
            continue

        # Multi-value endpoints (e.g., OHLC)
        if multi_value_map:
            for source_key, target_suffix in multi_value_map.items():
                val = row.get(source_key)
                if val is None:
                    continue
                if isinstance(val, dict):
                    for sub_key, sub_val in val.items():
                        suffix = multi_value_map.get(sub_key) or target_suffix
                        bucket_metric = f"{metric}:{suffix}"
                        _append_record(bucket_metric, ts, sub_val)
                else:
                    bucket_metric = f"{metric}:{target_suffix}"
                    _append_record(bucket_metric, ts, val)
            continue

        if value_key is None:
            continue
        if value_key not in row:
            continue
        val = row.get(value_key)
        # Breakdown endpoints: value is a dict of bucket -> value
        if isinstance(val, dict):
            for bucket_key, bucket_val in val.items():
                if bucket_val is None:
                    continue
                bucket_metric = f"{metric}:{bucket_key}"
                _append_record(bucket_metric, ts, bucket_val)
        # In case the API returns a list of dict buckets
        elif isinstance(val, list):
            for bucket in val:
                if not isinstance(bucket, dict):
                    continue
                for bucket_key, bucket_val in bucket.items():
                    if bucket_key in {timestamp_key, "t", "timestamp", "time"}:
                        continue
                    if bucket_val is None:
                        continue
                    bucket_metric = f"{metric}:{bucket_key}"
                    _append_record(bucket_metric, ts, bucket_val)
        else:
            _append_record(metric, ts, val)

    if not records:
        return pd.DataFrame(), None

    df = pd.DataFrame(records)
    if df.empty:
        return df, None

    df = df[["timestamp", "timeframe", "asset_symbol", "metric", "source", "value"]]
    df = df.drop_duplicates(subset=["timestamp", "timeframe", "asset_symbol", "metric"])
    return df, None


def fetch_and_store_glassnode_metrics(**context: Any) -> None:
    api_key = load_api_key(GLASSNODE_API_KEY_VAR)
    dry_run = is_dry_run()
    reset_full = RESET_IGNORE_HISTORY

    asset_filter_set: Optional[set] = None
    if ASSET_FILTER:
        asset_filter_set = {a.strip().lower() for a in ASSET_FILTER if a and a.strip()}
        logger.info("Asset filter active: %s", sorted(asset_filter_set))

    dry_run_asset: Optional[str] = None
    if dry_run:
        if asset_filter_set:
            dry_run_asset = sorted(asset_filter_set)[0]
            logger.info("Dry-run asset selected: %s", dry_run_asset)
        else:
            logger.info("Dry-run asset not limited (processing all assets).")
    session = requests.Session()
    failed_requests: List[str] = []
    missing_columns: List[str] = []
    total_rows_written = 0
    if reset_full:
        logger.info("Reset mode enabled: ignoring latest timestamps and fetching full history for all metrics.")
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
            multi_value_map = config.get("multi_value_map")
            interval_param = config.get("interval_param", "interval")
            since_param_name = config.get("since_param", "since")
            until_param_name = config.get("until_param", "until")

            # Allow limiting to a specific metric set for both dry-run and normal runs.
            if METRIC_FILTER and metric not in METRIC_FILTER:
                continue

            for asset in config["assets"]:
                asset_symbol = asset["asset_symbol"]
                # Apply asset filter in both dry-run and normal runs when configured.
                if asset_filter_set and asset_symbol not in asset_filter_set:
                    continue
                if dry_run and dry_run_asset and asset_symbol != dry_run_asset:
                    continue
                params = {**static_params, **asset.get("request_params", {})}
                # Rename interval key if the endpoint expects a short param (e.g., i)
                if "interval" in params and interval_param != "interval":
                    params[interval_param] = params.pop("interval")

                latest_ts = get_latest_timestamp(asset_symbol, metric, timeframe, source)
                effective_latest_ts = None if reset_full else latest_ts
                now_utc = datetime.now(timezone.utc)
                until_param = int(now_utc.timestamp())

                if dry_run:
                    # Constrain dry runs to recent data to avoid large pulls.
                    # If we have historical data, start from the next point; otherwise, use a short recent window.
                    if reset_full:
                        since_param = None
                    elif effective_latest_ts is not None:
                        next_point = build_since_parameter(effective_latest_ts, timeframe)
                        since_param = (
                            next_point
                            if next_point is not None
                            else int(effective_latest_ts.replace(tzinfo=timezone.utc).timestamp())
                        )
                    else:
                        since_param = int((now_utc - timedelta(days=DRY_RUN_LOOKBACK_DAYS)).timestamp())
                    if since_param is not None:
                        params[since_param_name] = since_param
                    params[until_param_name] = until_param
                    logger.info(
                        "Dry-run start window metric=%s asset=%s timeframe=%s since=%s",
                        metric,
                        asset_symbol,
                        timeframe,
                        datetime.fromtimestamp(since_param, tz=timezone.utc).strftime("%Y-%m-%d")
                        if since_param is not None
                        else "full",
                    )
                else:
                    since_param = None if reset_full else build_since_parameter(effective_latest_ts, timeframe)
                    if since_param:
                        params[since_param_name] = since_param
                    params[until_param_name] = until_param

                try:
                    raw_rows = make_glassnode_request(session, api_key, endpoint, params)
                    if raw_rows:
                        logger.debug(
                            "Sample raw row for metric=%s asset=%s: %s",
                            metric,
                            asset_symbol,
                            raw_rows[0],
                        )
                except AirflowException as exc:
                    logger.error(
                        "Failed to fetch metric %s for asset %s: %s. Continuing to next metric.",
                        metric,
                        asset_symbol,
                        exc,
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
                    latest_ts=effective_latest_ts,
                    multi_value_map=multi_value_map,
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
                    if dry_run:
                        logger.info(
                            "Prepared %s new rows for metric=%s asset=%s timeframe=%s (dry_run=True)",
                            len(df),
                            metric,
                            asset_symbol,
                            timeframe,
                        )
                    else:
                        logger.info(
                            "Prepared %s new rows for metric=%s asset=%s timeframe=%s",
                            len(df),
                            metric,
                            asset_symbol,
                            timeframe,
                        )
                        with rds_engine.begin() as conn:
                            logger.info(
                                "Writing %s rows to %s for metric=%s asset=%s timeframe=%s",
                                len(df),
                                TABLE_NAME,
                                metric,
                                asset_symbol,
                                timeframe,
                            )
                            df.to_sql(
                                TABLE_NAME,
                                con=conn,
                                if_exists="append",
                                index=False,
                                method=_upsert_do_nothing,
                                chunksize=1000,
                            )
                        total_rows_written += len(df)

                time.sleep(REQUEST_PAUSE_SECONDS)

        if total_rows_written == 0 and not dry_run:
            if failed_requests or missing_columns:
                logger.warning(
                    "No new Glassnode data ingested. Failures=%s missing_columns=%s",
                    failed_requests if failed_requests else "none",
                    missing_columns if missing_columns else "none",
                )
            else:
                logger.info("No new Glassnode data to ingest.")
            if ti:
                ti.xcom_push(
                    key="glassnode_warnings",
                    value={
                        "ingested_rows": 0,
                        "failed_requests": failed_requests,
                        "missing_columns": missing_columns,
                        "dry_run": dry_run,
                    },
                )
            return

        if dry_run:
            logger.info(
                "Dry-run enabled; skipping writes. Prepared rows were logged per metric.",
            )
            if ti:
                ti.xcom_push(
                    key="glassnode_warnings",
                    value={
                        "ingested_rows": 0,
                        "failed_requests": failed_requests,
                        "missing_columns": missing_columns,
                        "dry_run": True,
                    },
                )
            return

        logger.info("Inserted %s rows into %s", total_rows_written, TABLE_NAME)
        if failed_requests or missing_columns:
            logger.warning(
                "Completed with partial issues. Failed requests=%s; missing_columns=%s",
                failed_requests if failed_requests else "none",
                missing_columns if missing_columns else "none",
            )
        if ti:
            ti.xcom_push(
                key="glassnode_warnings",
                value={
                    "ingested_rows": total_rows_written,
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
    dag_id="glassnode_metrics_pipeline",
    default_args=default_args,
    description=get_dag_config("glassnode_metrics")["description"],
    schedule_interval=get_schedule_interval("glassnode_metrics"),
    start_date=get_start_date("glassnode_metrics"),
    catchup=False,
    max_active_runs=1,
) as dag:
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_glassnode_metrics",
        python_callable=fetch_and_store_glassnode_metrics,
        provide_context=True,
    )

    fetch_and_store_task
