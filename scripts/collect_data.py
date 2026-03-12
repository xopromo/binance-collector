"""
Crypto Data Collector — CoinGecko API
Собирает: тикеры (цена, объём, изменение 24ч) и свечи OHLCV (1ч).
Данные сохраняются в CSV-файлы с дедупликацией по timestamp.

Источник: CoinGecko (агрегирует данные с Binance, Bybit и др.)
Интервал свечей: 1h (минимум на бесплатном тарифе)
"""

import time
import yaml
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

CONFIG_PATH = Path("config.yaml")
DATA_PATH = Path("data")
COINGECKO = "https://api.coingecko.com/api/v3"

# Пауза между запросами (бесплатный тариф CoinGecko — 30 запросов/мин)
REQUEST_DELAY = 4.0  # CoinGecko free: 30 req/min → 2s min, 4s safe


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


def get(url: str, params: dict = None, timeout: int = 15) -> dict:
    headers = {"Accept": "application/json", "User-Agent": "crypto-collector/1.0"}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ─── ТИКЕРЫ (все сразу одним запросом) ──────────────────────────────────────

def collect_tickers(coins: list):
    """
    Один запрос — все 20 монет.
    Поля: цена, объём, изм. 24ч, max/min 24ч, market cap.
    """
    ids = ",".join(c["id"] for c in coins)
    id_to_symbol = {c["id"]: c["symbol"] for c in coins}
    try:
        items = get(f"{COINGECKO}/coins/markets", {
            "vs_currency": "usd",
            "ids": ids,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h",
        })
        ts = now_utc()
        for item in items:
            symbol = id_to_symbol.get(item["id"], item["symbol"].upper() + "USDT")
            row = {
                "timestamp":        ts,
                "price":            item["current_price"],
                "market_cap":       item["market_cap"],
                "volume_usdt_24h":  item["total_volume"],
                "change_pct_1h":    item.get("price_change_percentage_1h_in_currency", ""),
                "change_pct_24h":   item.get("price_change_percentage_24h_in_currency", ""),
                "high_24h":         item["high_24h"],
                "low_24h":          item["low_24h"],
                "ath":              item["ath"],
                "atl":              item["atl"],
            }
            append_csv(DATA_PATH / "tickers" / f"{symbol}.csv", pd.DataFrame([row]))
            print(f"  [OK] ticker {symbol}: ${item['current_price']}")
        time.sleep(REQUEST_DELAY)
    except Exception as e:
        print(f"  [ERR] tickers: {e}")
        time.sleep(REQUEST_DELAY)


# ─── OHLCV СВЕЧИ (1ч) ────────────────────────────────────────────────────────

def collect_ohlcv(coin_id: str, symbol: str):
    """
    Запрашивает OHLCV свечи за последние сутки.
    CoinGecko при days=1 возвращает 30-минутные свечи (48 штук).
    """
    try:
        data = get(f"{COINGECKO}/coins/{coin_id}/ohlc", {
            "vs_currency": "usd",
            "days": "1",
        })
        rows = []
        for k in data:
            ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            rows.append({
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open":      k[1],
                "high":      k[2],
                "low":       k[3],
                "close":     k[4],
            })
        if rows:
            append_csv(
                DATA_PATH / "ohlcv" / "30m" / f"{symbol}.csv",
                pd.DataFrame(rows)
            )
            print(f"  [OK] ohlcv {symbol} 30m: {len(rows)} candles")
        time.sleep(REQUEST_DELAY)
    except Exception as e:
        print(f"  [ERR] ohlcv {symbol}: {e}")
        time.sleep(REQUEST_DELAY)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    coins = config.get("coins", [])

    print(f"\n=== Crypto Collector (CoinGecko) | {now_utc()} ===")
    print(f"Coins: {len(coins)}\n")

    # Тикеры — один запрос на все монеты
    print("-- Tickers --")
    collect_tickers(coins)

    # OHLCV — по одному запросу на монету
    print("\n-- OHLCV 1h candles --")
    for coin in coins:
        collect_ohlcv(coin["id"], coin["symbol"])

    print(f"\n=== Done ===\n")


if __name__ == "__main__":
    main()
