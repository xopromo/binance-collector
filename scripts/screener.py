import json
import sys
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yaml

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from pair_filters import FILTERS, apply_filters, score_pair  # noqa: E402

# ── Config ────────────────────────────────────────────────────────────────────

with open(BASE / "config.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

ALL_SYMBOLS = cfg.get("spot_symbols", [])
INTERVAL    = cfg.get("intervals", ["5m"])[0]

CANDLES_PER_HOUR = {
    "1m": 60, "3m": 20, "5m": 12, "15m": 4,
    "30m": 2, "1h": 1, "2h": 1, "4h": 1,
}
N_CANDLES_1H = CANDLES_PER_HOUR.get(INTERVAL, 12)
COMMISSION_PCT = 0.1

SELECTED_PAIRS_FILE = BASE / "selected_pairs.yaml"


def load_selected_pairs() -> list[str]:
    if SELECTED_PAIRS_FILE.exists():
        with open(SELECTED_PAIRS_FILE, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        pairs = data.get("selected", [])
        if pairs:
            return pairs
    return ALL_SYMBOLS


def save_selected_pairs(symbols: list[str]):
    with open(SELECTED_PAIRS_FILE, "w", encoding="utf-8") as f:
        yaml.dump({"selected": symbols}, f, allow_unicode=True)
    # Синхронизируем config.yaml — коллектор подберёт новые пары автоматически
    config_path = BASE / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    config["spot_symbols"]    = symbols
    config["futures_symbols"] = symbols
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_csv(path: Path):
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception:
        return None


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


@st.cache_data(ttl=3600)
def fetch_tick_sizes(symbols: tuple) -> dict:
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/exchangeInfo",
            params={"symbols": json.dumps(list(symbols))},
            timeout=15,
        )
        r.raise_for_status()
        result = {}
        for s in r.json().get("symbols", []):
            for f in s.get("filters", []):
                if f["filterType"] == "PRICE_FILTER":
                    result[s["symbol"]] = float(f["tickSize"])
        return result
    except Exception:
        return {}


@st.cache_data(ttl=60)
def fetch_all_binance_usdt_tickers() -> pd.DataFrame:
    """Получить live 24ч тикеры для ВСЕХ USDT-пар Binance."""
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            timeout=20,
        )
        r.raise_for_status()
        rows = []
        for d in r.json():
            sym = d["symbol"]
            if not sym.endswith("USDT"):
                continue
            price = float(d["lastPrice"])
            if price <= 0:
                continue
            rows.append({
                "_symbol": sym,
                "Symbol":  sym.replace("USDT", ""),
                "Price":   price,
                "24h %":   float(d["priceChangePercent"]),
                "Vol 24h": float(d["quoteVolume"]),
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_all_usdt_tick_sizes() -> dict:
    """Получить тик-сайзы для ВСЕХ USDT спот-пар Binance."""
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/exchangeInfo",
            timeout=30,
        )
        r.raise_for_status()
        result = {}
        for s in r.json().get("symbols", []):
            if s["symbol"].endswith("USDT") and s.get("status") == "TRADING":
                for f in s.get("filters", []):
                    if f["filterType"] == "PRICE_FILTER":
                        result[s["symbol"]] = float(f["tickSize"])
        return result
    except Exception:
        return {}


def get_pairs_data_from_local() -> pd.DataFrame:
    """Fallback: строит данные по парам из локальных CSV когда Binance API недоступен."""
    tick_sizes = fetch_tick_sizes(tuple(ALL_SYMBOLS))
    rows = []
    for symbol in ALL_SYMBOLS:
        ticker = load_csv(BASE / "data" / "tickers" / f"{symbol}.csv")
        ohlcv  = load_csv(BASE / "data" / "ohlcv" / INTERVAL / f"{symbol}.csv")
        if ticker is None or ticker.empty:
            continue
        last  = ticker.iloc[-1]
        price = float(last.get("price", 0))
        vol   = float(last.get("volume_usdt_24h", 0))
        chg   = float(last.get("change_pct_24h", 0))
        tick_size = tick_sizes.get(symbol)
        if not tick_size or price <= 0:
            continue  # пара делистингована или нет данных о тиках
        tick_pct   = round(tick_size / price * 100, 6)
        comm_ticks = round(COMMISSION_PCT / tick_pct, 1)
        avg_range_pct = None
        if ohlcv is not None and len(ohlcv) >= 10 and {"high", "low", "close"}.issubset(ohlcv.columns):
            h = ohlcv["high"].astype(float).iloc[-20:]
            l = ohlcv["low"].astype(float).iloc[-20:]
            c = ohlcv["close"].astype(float).iloc[-20:]
            avg_range_pct = round(float(((h - l) / c * 100).mean()), 3)
        rows.append({
            "Symbol":      symbol.replace("USDT", ""),
            "_symbol":     symbol,
            "Price":       price,
            "24h %":       chg,
            "Vol 24h":     vol,
            "Tick %":      tick_pct,
            "Comm ticks":  comm_ticks,
            "Avg range %": avg_range_pct,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["Score"] = df.apply(score_pair, axis=1)
    return df


def get_all_pairs_data() -> pd.DataFrame:
    """Загружает данные для ВСЕХ USDT-пар Binance — используется в Выборе пар.

    Базовые метрики (цена, объём, тик) — из live API.
    Avg range % — из локальных CSV (только для уже собранных пар).
    """
    tickers_df = fetch_all_binance_usdt_tickers()
    if tickers_df.empty:
        return pd.DataFrame()

    tick_sizes = fetch_all_usdt_tick_sizes()
    rows = []
    for _, t in tickers_df.iterrows():
        symbol = t["_symbol"]
        price  = t["Price"]

        tick_size = tick_sizes.get(symbol)
        if not tick_size:
            continue  # пара не TRADING (делистинг/BREAK/HALT) — пропускаем
        tick_pct   = round(tick_size / price * 100, 6)
        comm_ticks = round(COMMISSION_PCT / tick_pct, 1)

        avg_range_pct = None
        ohlcv = load_csv(BASE / "data" / "ohlcv" / INTERVAL / f"{symbol}.csv")
        if ohlcv is not None and len(ohlcv) >= 10 and {"high", "low", "close"}.issubset(ohlcv.columns):
            h = ohlcv["high"].astype(float).iloc[-20:]
            l = ohlcv["low"].astype(float).iloc[-20:]
            c = ohlcv["close"].astype(float).iloc[-20:]
            avg_range_pct = round(float(((h - l) / c * 100).mean()), 3)

        rows.append({
            "Symbol":      t["Symbol"],
            "_symbol":     symbol,
            "Price":       price,
            "24h %":       t["24h %"],
            "Vol 24h":     t["Vol 24h"],
            "Tick %":      tick_pct,
            "Comm ticks":  comm_ticks,
            "Avg range %": avg_range_pct,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["Score"] = df.apply(score_pair, axis=1)
    return df


def get_screener_data(symbols: list[str]) -> pd.DataFrame:
    """Load full data only for active (selected) symbols."""
    tick_sizes = fetch_tick_sizes(tuple(symbols))
    rows = []
    for symbol in symbols:
        ticker  = load_csv(BASE / "data" / "tickers"        / f"{symbol}.csv")
        ohlcv   = load_csv(BASE / "data" / "ohlcv" / INTERVAL / f"{symbol}.csv")
        funding = load_csv(BASE / "data" / "futures" / "funding_rates" / f"{symbol}.csv")

        if ticker is None or ticker.empty:
            continue

        last      = ticker.iloc[-1]
        price     = float(last.get("price", 0))
        chg_24h   = float(last.get("change_pct_24h", 0))

        rsi = change_1h = vol_spike = funding_rate = None

        if ohlcv is not None and len(ohlcv) >= max(15, N_CANDLES_1H + 1):
            closes     = ohlcv["close"].astype(float)
            rsi_series = compute_rsi(closes)
            last_rsi   = rsi_series.iloc[-1]
            rsi        = round(float(last_rsi), 1) if not np.isnan(last_rsi) else None

            prev = float(closes.iloc[-(N_CANDLES_1H + 1)])
            curr = float(closes.iloc[-1])
            if prev > 0:
                change_1h = round((curr - prev) / prev * 100, 2)

            if "volume" in ohlcv.columns:
                vols    = ohlcv["volume"].astype(float)
                avg_vol = vols.iloc[-20:-1].mean()
                if avg_vol > 0:
                    vol_spike = round(float(vols.iloc[-1]) / avg_vol, 2)

        if funding is not None and not funding.empty:
            fr = funding.iloc[-1].get("funding_rate")
            if fr is not None:
                try:
                    funding_rate = round(float(fr) * 100, 4)
                except Exception:
                    pass

        tick_size  = tick_sizes.get(symbol)
        tick_pct   = round(tick_size / price * 100, 6) if tick_size and price > 0 else None
        comm_ticks = round(COMMISSION_PCT / tick_pct, 1) if tick_pct else None

        avg_range_pct = None
        if ohlcv is not None and len(ohlcv) >= 10 and {"high", "low", "close"}.issubset(ohlcv.columns):
            h = ohlcv["high"].astype(float).iloc[-20:]
            l = ohlcv["low"].astype(float).iloc[-20:]
            c = ohlcv["close"].astype(float).iloc[-20:]
            avg_range_pct = round(float(((h - l) / c * 100).mean()), 3)

        rows.append({
            "Symbol":      symbol.replace("USDT", ""),
            "Price":       price,
            "24h %":       chg_24h,
            "1h %":        change_1h,
            "RSI":         rsi,
            "Vol x":       vol_spike,
            "Fund %":      funding_rate,
            "Tick %":      tick_pct,
            "Comm ticks":  comm_ticks,
            "Avg range %": avg_range_pct,
            "Signals":     [],
        })

    return pd.DataFrame(rows)


# ── Style helpers ─────────────────────────────────────────────────────────────

def color_change(val):
    if pd.isna(val):
        return ""
    return "color: #00c853" if val > 0 else "color: #ff1744" if val < 0 else ""


def color_rsi(val):
    if pd.isna(val):
        return ""
    if val >= 70:
        return "color: #ff1744"
    if val <= 30:
        return "color: #00c853"
    return ""


def color_comm(val):
    if pd.isna(val):
        return ""
    if val <= 5:
        return "color: #00c853; font-weight: bold"
    if val <= 20:
        return "color: #ffeb3b"
    return "color: #ff1744"


# ── Streamlit app ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Binance Screener",
    layout="wide",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .signal-pump { background:#1b5e20;color:#69f0ae;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
    .signal-dump { background:#b71c1c;color:#ff8a80;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
    .signal-ob   { background:#bf360c;color:#ffccbc;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
    .signal-os   { background:#1a237e;color:#c5cae9;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
    .signal-vol  { background:#4a148c;color:#e1bee7;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
    .signal-fund { background:#263238;color:#cfd8dc;padding:2px 8px;border-radius:4px;margin:1px;font-size:12px;display:inline-block }
</style>
""", unsafe_allow_html=True)

# ── Session state init ────────────────────────────────────────────────────────

for key, default in [
    ("col_proc",     None),
    ("col_last",     None),
    ("col_auto",     False),
    ("col_auto_min", 5),
    ("active_symbols", load_selected_pairs()),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📈 Screener")
    active = st.session_state.active_symbols
    st.caption(f"Интервал: {INTERVAL}  |  Пар: {len(active)}/{len(ALL_SYMBOLS)}")
    refresh_sec = st.slider(
        "Авто-обновление (с)", 15, 120, 30, 5,
        key="refresh_sec",
        help="Как часто страница перезагружает данные из CSV. "
             "Меньше значение — страница обновляется чаще, но выше нагрузка на браузер."
    )
    st.divider()

    st.subheader("Фильтры")
    only_signals = st.checkbox(
        "Только с сигналами",
        key="only_signals",
        help="Показывать только пары с активными сигналами: "
             "PUMP/DUMP (движение цены), OB/OS (RSI), VOL (всплеск объёма), FUND (фандинг)."
    )
    min_abs_1h = st.slider(
        "Мин |1ч %|", 0.0, 10.0, 0.0, 0.5,
        key="min_abs_1h",
        help="Минимальное абсолютное изменение цены за последний час. "
             "Например, 1.0 — скрыть пары где цена изменилась менее чем на 1% за час. "
             "Помогает отфильтровать «стоячие» пары."
    )
    rsi_filter = st.select_slider(
        "Диапазон RSI", options=list(range(0, 101, 5)), value=(0, 100),
        key="rsi_filter",
        help="RSI (Relative Strength Index) — индикатор импульса цены от 0 до 100. "
             "< 30: перепроданность (возможен отскок вверх). "
             "> 70: перекупленность (возможна коррекция вниз). "
             "Установите диапазон, чтобы видеть только нужные зоны."
    )
    st.divider()

    st.subheader("Пороги сигналов")
    pump_thr = st.number_input(
        "Памп/Дамп %", value=3.0, step=0.5,
        key="pump_thr",
        help="Порог изменения цены за 1ч для генерации сигнала PUMP/DUMP. "
             "При значении 3.0: сигнал PUMP+ появляется при росте ≥3%, "
             "PUMP — при ≥4.5%, DUMP+ — при падении ≤-3%. "
             "Уменьшите для более чувствительных сигналов."
    )
    vol_thr = st.number_input(
        "Всплеск объёма x", value=3.0, step=0.5,
        key="vol_thr",
        help="Минимальный коэффициент всплеска объёма относительно среднего за 20 свечей. "
             "Например, 3.0 = объём в 3 раза выше нормы. "
             "Всплески объёма часто предшествуют резким движениям цены."
    )
    st.divider()

    # ── Collector controls ────────────────────────────────────────────────────
    st.subheader("Коллектор")

    proc       = st.session_state.col_proc
    is_running = proc is not None and proc.poll() is None

    if is_running:
        st.success("● Сбор данных...")
        if st.button("■ Стоп", use_container_width=True,
                     help="Остановить текущий процесс сбора данных."):
            proc.terminate()
            st.session_state.col_proc = None
            st.rerun()
    else:
        if proc is not None:
            st.session_state.col_proc = None
        if st.session_state.col_last:
            st.caption(f"Последний запуск: {st.session_state.col_last}")
        if st.button(
            "▶ Запустить", use_container_width=True, type="primary",
            help="Запускает collect_data.py — загружает для выбранных пар: "
                 "тикеры (цена, объём), свечи OHLCV (для RSI и диапазона), "
                 "фьючерсный фандинг и открытый интерес. "
                 "Первый запуск загружает 200 свечей (прогрев RSI), последующие — по 3."
        ):
            log_dir = BASE / "logs"
            log_dir.mkdir(exist_ok=True)
            with open(log_dir / "collect.log", "a") as log:
                p = subprocess.Popen(
                    [sys.executable, str(BASE / "scripts" / "collect_data.py")],
                    stdout=log, stderr=log, cwd=str(BASE),
                )
            st.session_state.col_proc = p
            st.session_state.col_last = pd.Timestamp.utcnow().strftime("%H:%M:%S UTC")
            st.rerun()

    st.divider()
    st.subheader("Обновление")
    if st.button(
        "⬇ Обновить скрипты", use_container_width=True,
        help="Скачивает последнюю версию screener.py и pair_filters.py с GitHub "
             "и автоматически перезапускает скринер. "
             "Используй, когда вышло обновление."
    ):
        bat = BASE / "update.bat"
        try:
            subprocess.Popen(["cmd", "/c", str(bat)], cwd=str(BASE))
            st.success("Обновление запущено — скринер перезапустится автоматически.")
        except Exception as e:
            st.error(f"Ошибка: {e}")
    st.divider()

    st.session_state.col_auto = st.checkbox(
        "Авто-сбор", value=st.session_state.col_auto,
        help="Автоматически запускать коллектор через заданный интервал. "
             "Удобно для постоянного мониторинга: данные будут всегда актуальными "
             "без ручного нажатия «Запустить»."
    )
    if st.session_state.col_auto:
        st.session_state.col_auto_min = st.slider(
            "Каждые (мин)", 1, 60, st.session_state.col_auto_min,
            help="Интервал автоматического запуска коллектора. "
                 "Рекомендуется 5–15 мин для скальпинга, 30–60 мин для свинга."
        )



@st.fragment(run_every=refresh_sec)
def live_view():
    # Read sidebar filter values (widgets store state via key= parameter)
    only_signals = st.session_state.get("only_signals", False)
    min_abs_1h   = st.session_state.get("min_abs_1h", 0.0)
    rsi_filter   = st.session_state.get("rsi_filter", (0, 100))
    pump_thr     = st.session_state.get("pump_thr", 3.0)
    vol_thr      = st.session_state.get("vol_thr", 3.0)

    # ── Load data ─────────────────────────────────────────────────────────────────

    with st.spinner("Загрузка данных..."):
        df = get_screener_data(st.session_state.active_symbols)

    if df.empty:
        st.error("Нет данных. Сначала запустите коллектор.")
        st.stop()

    # ── Recompute signals with sidebar thresholds ─────────────────────────────────

    def recompute_signals(row):
        sigs, c1h, rsi, vol, fr = [], row["1h %"], row["RSI"], row["Vol x"], row["Fund %"]
        if pd.notna(c1h):
            if c1h >= pump_thr * 1.5:    sigs.append(("PUMP",  "pump"))
            elif c1h >= pump_thr:         sigs.append(("PUMP+", "pump"))
            elif c1h <= -pump_thr * 1.5:  sigs.append(("DUMP",  "dump"))
            elif c1h <= -pump_thr:        sigs.append(("DUMP+", "dump"))
        if pd.notna(rsi):
            if rsi >= 75:    sigs.append(("OVERBOUGHT", "ob"))
            elif rsi >= 70:  sigs.append(("OB",         "ob"))
            elif rsi <= 25:  sigs.append(("OVERSOLD",   "os"))
            elif rsi <= 30:  sigs.append(("OS",         "os"))
        if pd.notna(vol) and vol >= vol_thr:
            sigs.append((f"VOL x{vol:.1f}", "vol"))
        if pd.notna(fr):
            if fr >= 0.05:    sigs.append(("FUND+", "fund"))
            elif fr <= -0.05: sigs.append(("FUND-", "fund"))
        return sigs

    df["Signals"] = df.apply(recompute_signals, axis=1)

    # ── Apply screener filters ────────────────────────────────────────────────────

    filtered = df.copy()
    if only_signals:
        filtered = filtered[filtered["Signals"].apply(len) > 0]
    if min_abs_1h > 0:
        filtered = filtered[filtered["1h %"].abs() >= min_abs_1h]
    filtered = filtered[
        filtered["RSI"].isna() |
        ((filtered["RSI"] >= rsi_filter[0]) & (filtered["RSI"] <= rsi_filter[1]))
    ]

    # ── Main layout ───────────────────────────────────────────────────────────────

    col_t, col_r = st.columns([3, 1])
    with col_t:
        st.title("📈 Binance Screener")
    with col_r:
        st.metric("Обновлено", pd.Timestamp.utcnow().strftime("%H:%M:%S UTC"))

    # ── Alerts ────────────────────────────────────────────────────────────────────

    alert_rows = filtered[filtered["Signals"].apply(len) > 0]
    if not alert_rows.empty:
        st.subheader(f"🚨 Сигналы ({len(alert_rows)})")
        cols = st.columns(min(len(alert_rows), 4))
        for i, (_, row) in enumerate(alert_rows.iterrows()):
            with cols[i % 4]:
                tags_html = " ".join(
                    f'<span class="signal-{css}">{label}</span>'
                    for label, css in row["Signals"]
                )
                c1h     = f"{row['1h %']:+.2f}%" if pd.notna(row["1h %"]) else "—"
                rsi_str = f"RSI {row['RSI']:.0f}" if pd.notna(row["RSI"]) else ""
                tv_url  = f"https://www.tradingview.com/chart/?symbol=BYBIT:{row['_symbol']}.P"
                st.markdown(
                    f"**{row['Symbol']}** `${row['Price']:,.4f}` "
                    f"<a href='{tv_url}' target='_blank' style='font-size:12px;text-decoration:none;'>↗ TV</a>  \n"
                    f"1h: {c1h} | {rsi_str}  \n{tags_html}",
                    unsafe_allow_html=True,
                )
    else:
        st.info("Нет сигналов при текущих порогах.")

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────────────────

    tab_screener, tab_selector, tab_tick = st.tabs([
        f"Скринер ({len(filtered)})",
        f"Выбор пар ({len(st.session_state.active_symbols)}/{len(ALL_SYMBOLS)})",
        "Тик / Комиссия",
    ])

    # ── Tab 1: Screener ───────────────────────────────────────────────────────────

    with tab_screener:
        def fmt_signal(sigs):
            return " ".join(l for l, _ in sigs) if sigs else "—"

        display = filtered.copy()
        display["Signals"] = display["Signals"].apply(fmt_signal)
        cols_main = ["Symbol", "Price", "24h %", "1h %", "RSI", "Vol x", "Fund %", "Signals"]
        styled = (
            display[cols_main].style
            .applymap(color_change, subset=["24h %", "1h %"])
            .applymap(color_rsi,    subset=["RSI"])
            .format({
                "Price":  lambda v: f"${v:,.4f}" if pd.notna(v) else "—",
                "24h %":  lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
                "1h %":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
                "RSI":    lambda v: f"{v:.1f}"   if pd.notna(v) else "—",
                "Vol x":  lambda v: f"{v:.2f}x"  if pd.notna(v) else "—",
                "Fund %": lambda v: f"{v:+.4f}%" if pd.notna(v) else "—",
            })
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)
        st.caption(f"Данные: {BASE/'data'}  |  Интервал: {INTERVAL}  |  Обновление: {refresh_sec}с")

    # ── Tab 2: Pair Selector ──────────────────────────────────────────────────────

    with tab_selector:
        st.subheader("Выбор пар по критериям")
        st.caption(
            "Данные берутся **live с Binance API** — доступны все USDT-пары. "
            "Avg range % считается только для пар, по которым уже собраны данные локально. "
            "Настройте фильтры → нажмите **Применить** → коллектор начнёт собирать выбранные пары."
        )

        # ── Filter controls (from registry) ──────────────────────────────────────
        filter_settings: dict[str, float] = {}

        with st.expander("Фильтры", expanded=True):
            for f in FILTERS:
                lo, hi = f["range"]
                val = st.slider(
                    f["label"],
                    min_value=float(lo),
                    max_value=float(hi),
                    value=float(f["default"]),
                    step=float(f["step"]),
                    help=f["description"],
                    key=f"fslider_{f['id']}",
                )
                filter_settings[f["id"]] = val

        # ── Load all pairs and apply filters ─────────────────────────────────────
        with st.spinner("Загрузка данных с Binance API..."):
            all_df = get_all_pairs_data()

        api_failed = all_df.empty
        if api_failed:
            st.warning(
                "⚠️ Нет ответа от Binance API — возможно, ограничен доступ (VPN/прокси/регион). "
                "Показываются локально собранные данные."
            )
            col_retry, _ = st.columns([1, 3])
            with col_retry:
                if st.button("🔄 Повторить запрос", help="Очистить кэш и заново запросить данные с Binance API."):
                    fetch_all_binance_usdt_tickers.clear()
                    fetch_all_usdt_tick_sizes.clear()
                    st.rerun()
            all_df = get_pairs_data_from_local()
            if all_df.empty:
                st.error(
                    "Локальных данных тоже нет. "
                    "Сначала запустите коллектор (кнопка «▶ Запустить» в сайдбаре) "
                    "чтобы собрать данные по парам из config.yaml."
                )
                st.stop()
        else:
            passed = apply_filters(all_df, filter_settings)
            passed = passed.sort_values("Score", ascending=True, na_position="last")

            n_pass = len(passed)
            n_fail = len(all_df) - n_pass

            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Всего на Binance", len(all_df))
            col_b.metric("Проходят фильтры", n_pass, delta=f"-{n_fail} отсеяно")
            col_c.metric("Собирается сейчас", len(st.session_state.active_symbols))

            # Show table with pass/fail
            preview = all_df.copy()
            preview["✓"] = preview["_symbol"].isin(passed["_symbol"]).map(
                {True: "✅", False: "❌"}
            )
            preview = preview.sort_values("Score", ascending=True, na_position="last")

            def color_pass(val):
                return "color: #00c853" if val == "✅" else "color: #444"

            styled_sel = (
                preview[["✓", "Symbol", "Comm ticks", "Avg range %", "Vol 24h", "Tick %", "Score"]].style
                .applymap(color_pass,  subset=["✓"])
                .applymap(color_comm,  subset=["Comm ticks"])
                .format({
                    "Comm ticks":  lambda v: f"{v:.1f}"        if pd.notna(v) else "—",
                    "Avg range %": lambda v: f"{v:.3f}%"       if pd.notna(v) else "—",
                    "Vol 24h":     lambda v: f"${v:,.0f}"      if pd.notna(v) else "—",
                    "Tick %":      lambda v: f"{v:.4f}%"       if pd.notna(v) else "—",
                    "Score":       lambda v: f"{v:.1f}"        if pd.notna(v) else "—",
                })
            )
            col_tbl, col_score_help = st.columns([11, 1])
            with col_score_help:
                with st.popover("ℹ️"):
                    st.markdown(
                        "**Score = Comm ticks × 2 − Avg range % × 10**\n\n"
                        "Чем **меньше** Score — тем лучше пара для скальпинга.\n\n"
                        "- **Comm ticks** — сколько тиков нужно пройти, чтобы отбить комиссию. "
                        "Меньше = дешевле торговать.\n"
                        "- **Avg range %** — средний диапазон свечи за 20 баров. "
                        "Больше = пара активнее двигается.\n\n"
                        "Весовые коэффициенты: Comm ticks ×2 важнее, Avg range ×10 бонус за волатильность."
                    )
            with col_tbl:
                st.dataframe(styled_sel, use_container_width=True, hide_index=True)

            # ── Apply / Reset buttons ─────────────────────────────────────────────
            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    f"✅ Применить — собирать {n_pass} пар",
                    type="primary",
                    use_container_width=True,
                    disabled=n_pass == 0,
                ):
                    selected = passed["_symbol"].tolist()
                    st.session_state.active_symbols = selected
                    save_selected_pairs(selected)
                    st.success(f"Применено: {n_pass} пар сохранены. Коллектор подберёт их при следующем запуске.")
                    st.rerun()
            with col2:
                if st.button(
                    f"↺ Сбросить — все {len(ALL_SYMBOLS)} пар",
                    use_container_width=True,
                ):
                    st.session_state.active_symbols = ALL_SYMBOLS
                    save_selected_pairs(ALL_SYMBOLS)
                    st.rerun()

            st.caption(
                "Score = Comm ticks × 2 − Avg range % × 10  |  Меньше Score — лучше пара для торговли"
            )

    # ── Tab 3: Tick / Commission ──────────────────────────────────────────────────

    with tab_tick:
        st.caption(
            f"Tick % = tick_size / price × 100  |  "
            f"Comm ticks = {COMMISSION_PCT}% / tick%  |  "
            f"Avg range = средний диапазон свечи (20 баров)"
        )
        tick_df = df[["Symbol", "Price", "Tick %", "Comm ticks", "Avg range %", "24h %"]].copy()
        tick_df = tick_df.sort_values("Comm ticks", ascending=True, na_position="last")

        styled_tick = (
            tick_df.style
            .applymap(color_comm, subset=["Comm ticks"])
            .format({
                "Price":       lambda v: f"${v:,.4f}" if pd.notna(v) else "—",
                "Tick %":      lambda v: f"{v:.4f}%"  if pd.notna(v) else "—",
                "Comm ticks":  lambda v: f"{v:.1f}"   if pd.notna(v) else "—",
                "Avg range %": lambda v: f"{v:.3f}%"  if pd.notna(v) else "—",
                "24h %":       lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            })
        )
        st.dataframe(styled_tick, use_container_width=True, hide_index=True)
        st.caption("🟢 Comm ticks ≤ 5 — отлично  |  🟡 ≤ 20 — приемлемо  |  🔴 > 20 — комиссия существенна")

    # ── Auto-collect ──────────────────────────────────────────────────────────────

    if st.session_state.col_auto:
        proc       = st.session_state.col_proc
        is_running = proc is not None and proc.poll() is None
        if not is_running:
            last         = st.session_state.col_last
            interval_sec = st.session_state.col_auto_min * 60
            needs_run    = last is None or (
                pd.Timestamp.utcnow()
                - pd.Timestamp(last.replace(" UTC", ""), tz="UTC")
            ).total_seconds() >= interval_sec
            if needs_run:
                log_dir = BASE / "logs"
                log_dir.mkdir(exist_ok=True)
                with open(log_dir / "collect.log", "a") as log:
                    p = subprocess.Popen(
                        [sys.executable, str(BASE / "scripts" / "collect_data.py")],
                        stdout=log, stderr=log, cwd=str(BASE),
                    )
                st.session_state.col_proc = p
                st.session_state.col_last = pd.Timestamp.utcnow().strftime("%H:%M:%S UTC")


live_view()
