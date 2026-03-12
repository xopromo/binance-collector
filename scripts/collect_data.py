"""
Crypto Data Collector — Bybit API
Собирает: спотовые тикеры, OHLCV свечи, фьючерсный фандинг и открытый интерес.
Данные сохраняются в CSV-файлы с дедупликацией по timestamp.

Примечание: использует Bybit вместо Binance — те же самые пары (BTCUSDT, ETHUSDT и т.д.),
те же типы данных. Binance блокирует запросы с серверов США (GitHub Actions).
"""

import time
import yaml
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

CONFIG_PATH = Path("config.yaml")
DATA_PATH = Path("data")

BYBIT_API = "https://api.bybit.com"

# Перевод таймфреймов из стандартных в формат Bybit
INTERVAL_MAP = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
}


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


def get(url: str, params: dict = None, timeout: int = 10) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ─── SPOT: ТИКЕРЫ ────────────────────────────────────────────────────────────

def collect_spot_ticker(symbol: str):
    try:
        d = get(f"{BYBIT_API}/v5/market/tickers",
                {"category": "spot", "symbol": symbol})
        item = d["result"]["list"][0]
        row = {
            "timestamp":       now_utc(),
            "price":           item["lastPrice"],
            "volume_usdt_24h": item["turnover24h"],
            "change_pct_24h":  item["price24hPcnt"],
            "high_24h":        item["highPrice24h"],
            "low_24h":         item["lowPrice24h"],
            "volume_24h":      item["volume24h"],
        }
        append_csv(DATA_PATH / "spot" / "tickers" / f"{symbol}.csv", pd.DataFrame([row]))
        print(f"  [OK] ticker {symbol}: {item['lastPrice']}")
    except Exception as e:
        print(f"  [ERR] ticker {symbol}: {e}")


# ─── SPOT: СВЕЧИ OHLCV ───────────────────────────────────────────────────────

def collect_ohlcv(symbol: str, interval: str):
    bybit_interval = INTERVAL_MAP.get(interval, interval)
    try:
        # limit=3 — берём последние 3 свечи, последнюю (незакрытую) отбрасываем
        d = get(f"{BYBIT_API}/v5/market/kline",
                {"category": "spot", "symbol": symbol,
                 "interval": bybit_interval, "limit": 3})
        rows = []
        # Bybit возвращает в порядке убывания времени, пропускаем первую (незакрытую)
        for k in d["result"]["list"][1:]:
            open_time = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc)
            rows.append({
                "timestamp":    open_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open":         k[1],
                "high":         k[2],
                "low":          k[3],
                "close":        k[4],
                "volume":       k[5],
                "quote_volume": k[6],
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
        # Текущий тикер фьючерса — содержит mark price, index price, funding rate
        d = get(f"{BYBIT_API}/v5/market/tickers",
                {"category": "linear", "symbol": symbol})
        item = d["result"]["list"][0]
        next_ts = int(item.get("nextFundingTime", 0))
        next_funding = (
            datetime.fromtimestamp(next_ts / 1000, tz=timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
            if next_ts else ""
        )
        row = {
            "timestamp":         now_utc(),
            "mark_price":        item.get("markPrice", ""),
            "index_price":       item.get("indexPrice", ""),
            "funding_rate":      item.get("fundingRate", ""),
            "next_funding_time": next_funding,
        }
        append_csv(
            DATA_PATH / "futures" / "funding_rates" / f"{symbol}.csv",
            pd.DataFrame([row])
        )
        print(f"  [OK] funding {symbol}: {item.get('fundingRate', 'N/A')}")
    except Exception as e:
        print(f"  [ERR] funding {symbol}: {e}")


# ─── FUTURES: ОТКРЫТЫЙ ИНТЕРЕС ────────────────────────────────────────────────

def collect_open_interest(symbol: str):
    try:
        d = get(f"{BYBIT_API}/v5/market/open-interest",
                {"category": "linear", "symbol": symbol,
                 "intervalTime": "5min", "limit": 1})
        items = d["result"]["list"]
        if not items:
            print(f"  [SKIP] OI {symbol}: no data")
            return
        item = items[0]
        ts = datetime.fromtimestamp(int(item["timestamp"]) / 1000, tz=timezone.utc)
        row = {
            "timestamp":     ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "open_interest": item["openInterest"],
        }
        append_csv(
            DATA_PATH / "futures" / "open_interest" / f"{symbol}.csv",
            pd.DataFrame([row])
        )
        print(f"  [OK] OI {symbol}: {item['openInterest']}")
    except Exception as e:
        print(f"  [ERR] OI {symbol}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    spot_symbols    = config.get("spot_symbols", [])
    futures_symbols = config.get("futures_symbols", [])
    intervals       = config.get("intervals", ["5m"])

    print(f"\n=== Crypto Collector (Bybit) | {now_utc()} ===")
    print(f"Spot: {len(spot_symbols)} | Futures: {len(futures_symbols)} | Intervals: {intervals}\n")

    print("-- Spot tickers --")
    for symbol in spot_symbols:
        collect_spot_ticker(symbol)
        time.sleep(0.1)

    print("\n-- OHLCV candles --")
    for symbol in spot_symbols:
        for interval in intervals:
            collect_ohlcv(symbol, interval)
            time.sleep(0.1)

    print("\n-- Futures funding rates --")
    for symbol in futures_symbols:
        collect_futures_funding(symbol)
        time.sleep(0.1)

    print("\n-- Futures open interest --")
    for symbol in futures_symbols:
        collect_open_interest(symbol)
        time.sleep(0.1)

    print(f"\n=== Done ===\n")


if __name__ == "__main__":
    main()
