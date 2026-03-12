"""
Binance Data Collector — через Tor-прокси (обход US geo-block)
Собирает: спотовые тикеры, OHLCV 5m свечи, фьючерсный фандинг и открытый интерес.
Данные сохраняются в CSV-файлы с дедупликацией по timestamp.
"""

import os
import time
import yaml
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

CONFIG_PATH = Path("config.yaml")
DATA_PATH = Path("data")

BINANCE_SPOT    = "https://api.binance.com"
BINANCE_FUTURES = "https://fapi.binance.com"

# Tor SOCKS5 прокси — направляет трафик через EU/Asia, где Binance не заблокирован
TOR_PROXY = "socks5h://127.0.0.1:9050"
PROXIES = {"http": TOR_PROXY, "https": TOR_PROXY}

REQUEST_DELAY = 0.3  # секунды между запросами


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def append_csv(filepath: Path, new_df: pd.DataFrame, key_col: str = "timestamp"):
    """Добавляет новые строки в CSV, убирая дубликаты по key_col."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if filepath.exists() and filepath.stat().st_size > 0:
        existing = pd.read_csv(filepath, dtype=str)
        combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
        combined = combined.drop_duplicates(subset=[key_col], keep="last")
        combined = combined.sort_values(key_col)
    else:
        combined = new_df.astype(str)
    combined.to_csv(filepath, index=False)


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get(url: str, params: dict = None, timeout: int = 30) -> dict | list:
    r = requests.get(url, params=params, proxies=PROXIES, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ─── SPOT: ТИКЕРЫ ────────────────────────────────────────────────────────────

def collect_spot_ticker(symbol: str):
    try:
        d = get(f"{BINANCE_SPOT}/api/v3/ticker/24hr", {"symbol": symbol})
        row = {
            "timestamp":       now_utc(),
            "price":           d["lastPrice"],
            "volume_usdt_24h": d["quoteVolume"],
            "change_pct_24h":  d["priceChangePercent"],
            "high_24h":        d["highPrice"],
            "low_24h":         d["lowPrice"],
            "trades_24h":      d["count"],
        }
        append_csv(DATA_PATH / "spot" / "tickers" / f"{symbol}.csv", pd.DataFrame([row]))
        print(f"  [OK] ticker {symbol}: {d['lastPrice']}")
    except Exception as e:
        print(f"  [ERR] ticker {symbol}: {e}")


# ─── SPOT: СВЕЧИ OHLCV ───────────────────────────────────────────────────────

def collect_ohlcv(symbol: str, interval: str):
    try:
        # limit=3: берём последние 3 свечи, отбрасываем незакрытую последнюю
        data = get(f"{BINANCE_SPOT}/api/v3/klines",
                   {"symbol": symbol, "interval": interval, "limit": 3})
        rows = []
        for k in data[:-1]:
            open_time = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            rows.append({
                "timestamp":    open_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open":         k[1],
                "high":         k[2],
                "low":          k[3],
                "close":        k[4],
                "volume":       k[5],
                "quote_volume": k[7],
                "trades":       k[8],
            })
        if rows:
            append_csv(
                DATA_PATH / "spot" / "ohlcv" / interval / f"{symbol}.csv",
                pd.DataFrame(rows)
            )
            print(f"  [OK] ohlcv {symbol} {interval}: +{len(rows)} candle(s)")
    except Exception as e:
        print(f"  [ERR] ohlcv {symbol} {interval}: {e}")


# ─── FUTURES: ФАНДИНГ + MARK PRICE ───────────────────────────────────────────

def collect_futures_funding(symbol: str):
    try:
        d = get(f"{BINANCE_FUTURES}/fapi/v1/premiumIndex", {"symbol": symbol})
        next_ts = int(d.get("nextFundingTime", 0))
        next_funding = (
            datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
            if next_ts else ""
        )
        row = {
            "timestamp":         now_utc(),
            "mark_price":        d["markPrice"],
            "index_price":       d["indexPrice"],
            "funding_rate":      d["lastFundingRate"],
            "next_funding_time": next_funding,
        }
        append_csv(
            DATA_PATH / "futures" / "funding_rates" / f"{symbol}.csv",
            pd.DataFrame([row])
        )
        print(f"  [OK] funding {symbol}: {d['lastFundingRate']}")
    except Exception as e:
        print(f"  [ERR] funding {symbol}: {e}")


# ─── FUTURES: ОТКРЫТЫЙ ИНТЕРЕС ────────────────────────────────────────────────

def collect_open_interest(symbol: str):
    try:
        d = get(f"{BINANCE_FUTURES}/fapi/v1/openInterest", {"symbol": symbol})
        row = {
            "timestamp":     now_utc(),
            "open_interest": d["openInterest"],
        }
        append_csv(
            DATA_PATH / "futures" / "open_interest" / f"{symbol}.csv",
            pd.DataFrame([row])
        )
        print(f"  [OK] OI {symbol}: {d['openInterest']}")
    except Exception as e:
        print(f"  [ERR] OI {symbol}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    spot_symbols    = config.get("spot_symbols", [])
    futures_symbols = config.get("futures_symbols", [])
    intervals       = config.get("intervals", ["5m"])

    print(f"\n=== Binance Collector (via Tor) | {now_utc()} ===")
    print(f"Spot: {len(spot_symbols)} | Futures: {len(futures_symbols)} | Intervals: {intervals}\n")

    print("-- Spot tickers --")
    for symbol in spot_symbols:
        collect_spot_ticker(symbol)
        time.sleep(REQUEST_DELAY)

    print("\n-- OHLCV candles --")
    for symbol in spot_symbols:
        for interval in intervals:
            collect_ohlcv(symbol, interval)
            time.sleep(REQUEST_DELAY)

    print("\n-- Futures funding rates --")
    for symbol in futures_symbols:
        collect_futures_funding(symbol)
        time.sleep(REQUEST_DELAY)

    print("\n-- Futures open interest --")
    for symbol in futures_symbols:
        collect_open_interest(symbol)
        time.sleep(REQUEST_DELAY)

    print(f"\n=== Done ===\n")


if __name__ == "__main__":
    main()
