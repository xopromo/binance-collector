import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import yaml
import time

BASE = Path(__file__).parent.parent

with open(BASE / "config.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

SYMBOLS = cfg.get("spot_symbols", [])
INTERVAL = cfg.get("intervals", ["5m"])[0]

# Candles per 1 hour for each interval
CANDLES_PER_HOUR = {
    "1m": 60, "3m": 20, "5m": 12, "15m": 4,
    "30m": 2, "1h": 1, "2h": 1, "4h": 1,
}
N_CANDLES_1H = CANDLES_PER_HOUR.get(INTERVAL, 12)


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def load_csv(path: Path):
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception:
        return None


def get_screener_data() -> pd.DataFrame:
    rows = []
    for symbol in SYMBOLS:
        ticker = load_csv(BASE / "data" / "tickers" / f"{symbol}.csv")
        ohlcv = load_csv(BASE / "data" / "ohlcv" / INTERVAL / f"{symbol}.csv")
        funding = load_csv(BASE / "data" / "futures" / "funding_rates" / f"{symbol}.csv")

        if ticker is None or ticker.empty:
            continue

        last = ticker.iloc[-1]
        price = float(last.get("price", 0))
        change_24h = float(last.get("change_pct_24h", 0))

        rsi = None
        change_1h = None
        vol_spike = None
        funding_rate = None

        if ohlcv is not None and len(ohlcv) >= max(15, N_CANDLES_1H + 1):
            closes = ohlcv["close"].astype(float)

            rsi_series = compute_rsi(closes)
            last_rsi = rsi_series.iloc[-1]
            rsi = round(float(last_rsi), 1) if not np.isnan(last_rsi) else None

            prev = float(closes.iloc[-(N_CANDLES_1H + 1)])
            curr = float(closes.iloc[-1])
            if prev > 0:
                change_1h = round((curr - prev) / prev * 100, 2)

            if "volume" in ohlcv.columns:
                vols = ohlcv["volume"].astype(float)
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

        signals = []
        if change_1h is not None:
            if change_1h >= 5:
                signals.append("PUMP")
            elif change_1h >= 3:
                signals.append("PUMP+")
            elif change_1h <= -5:
                signals.append("DUMP")
            elif change_1h <= -3:
                signals.append("DUMP+")
        if rsi is not None:
            if rsi >= 75:
                signals.append("OVERBOUGHT")
            elif rsi >= 70:
                signals.append("OB")
            elif rsi <= 25:
                signals.append("OVERSOLD")
            elif rsi <= 30:
                signals.append("OS")
        if vol_spike is not None and vol_spike >= 3:
            signals.append(f"VOL x{vol_spike:.1f}")
        if funding_rate is not None:
            if funding_rate >= 0.05:
                signals.append("FUND+")
            elif funding_rate <= -0.05:
                signals.append("FUND-")

        rows.append({
            "Symbol": symbol.replace("USDT", ""),
            "Price": price,
            "24h %": change_24h,
            "1h %": change_1h,
            "RSI": rsi,
            "Vol x": vol_spike,
            "Fund %": funding_rate,
            "Signals": signals,
        })

    return pd.DataFrame(rows)


def color_change(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    return "color: #00c853" if val > 0 else "color: #ff1744" if val < 0 else ""


def color_rsi(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    if val >= 70:
        return "color: #ff1744"
    if val <= 30:
        return "color: #00c853"
    return ""


# ── Streamlit app ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Binance Screener",
    layout="wide",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .signal-pump { background: #1b5e20; color: #69f0ae; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .signal-dump { background: #b71c1c; color: #ff8a80; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .signal-ob   { background: #bf360c; color: #ffccbc; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .signal-os   { background: #1a237e; color: #c5cae9; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .signal-vol  { background: #4a148c; color: #e1bee7; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .signal-fund { background: #263238; color: #cfd8dc; padding: 2px 8px; border-radius: 4px; margin: 1px; font-size: 12px; display: inline-block; }
    .alert-card  { border-left: 4px solid; padding: 8px 12px; margin: 4px 0; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📈 Screener")
    st.caption(f"Interval: {INTERVAL}")
    refresh_sec = st.slider("Auto-refresh (s)", 15, 120, 30, 5)
    st.divider()

    st.subheader("Filters")
    only_signals = st.checkbox("Only with signals")
    min_abs_1h = st.slider("Min |1h %|", 0.0, 10.0, 0.0, 0.5)
    rsi_filter = st.select_slider(
        "RSI range",
        options=list(range(0, 101, 5)),
        value=(0, 100),
    )
    st.divider()

    st.subheader("Signal thresholds")
    pump_thr = st.number_input("Pump/Dump %", value=3.0, step=0.5)
    vol_thr = st.number_input("Volume spike x", value=3.0, step=0.5)

# ── Load data ─────────────────────────────────────────────────────────────────
placeholder = st.empty()

with st.spinner("Loading data..."):
    df = get_screener_data()

if df.empty:
    st.error("No data found. Run collect_data.py first.")
    st.stop()

# Re-compute signals with custom thresholds
def recompute_signals(row):
    signals = []
    c1h = row["1h %"]
    rsi = row["RSI"]
    vol = row["Vol x"]
    fr = row["Fund %"]
    if pd.notna(c1h):
        if c1h >= pump_thr * 1.5:   signals.append(("PUMP",  "pump"))
        elif c1h >= pump_thr:        signals.append(("PUMP+", "pump"))
        elif c1h <= -pump_thr * 1.5: signals.append(("DUMP",  "dump"))
        elif c1h <= -pump_thr:       signals.append(("DUMP+", "dump"))
    if pd.notna(rsi):
        if rsi >= 75:   signals.append(("OVERBOUGHT", "ob"))
        elif rsi >= 70: signals.append(("OB",          "ob"))
        elif rsi <= 25: signals.append(("OVERSOLD",    "os"))
        elif rsi <= 30: signals.append(("OS",          "os"))
    if pd.notna(vol) and vol >= vol_thr:
        signals.append((f"VOL x{vol:.1f}", "vol"))
    if pd.notna(fr):
        if fr >= 0.05:   signals.append(("FUND+", "fund"))
        elif fr <= -0.05: signals.append(("FUND-", "fund"))
    return signals

df["Signals"] = df.apply(recompute_signals, axis=1)

# ── Apply filters ──────────────────────────────────────────────────────────────
filtered = df.copy()
if only_signals:
    filtered = filtered[filtered["Signals"].apply(len) > 0]
if min_abs_1h > 0:
    filtered = filtered[filtered["1h %"].abs() >= min_abs_1h]
filtered = filtered[
    (filtered["RSI"].isna()) |
    ((filtered["RSI"] >= rsi_filter[0]) & (filtered["RSI"] <= rsi_filter[1]))
]

with placeholder.container():
    # ── Header ─────────────────────────────────────────────────────────────────
    col_t, col_r = st.columns([3, 1])
    with col_t:
        st.title("📈 Binance Screener")
    with col_r:
        now = pd.Timestamp.utcnow().strftime("%H:%M:%S UTC")
        st.metric("Last update", now)

    # ── Alerts ─────────────────────────────────────────────────────────────────
    alert_rows = filtered[filtered["Signals"].apply(len) > 0]
    if not alert_rows.empty:
        st.subheader(f"🚨 Signals ({len(alert_rows)})")
        cols = st.columns(min(len(alert_rows), 4))
        for i, (_, row) in enumerate(alert_rows.iterrows()):
            with cols[i % 4]:
                tags_html = " ".join(
                    f'<span class="signal-{css}">{label}</span>'
                    for label, css in row["Signals"]
                )
                c1h = f"{row['1h %']:+.2f}%" if pd.notna(row["1h %"]) else "—"
                rsi_str = f"RSI {row['RSI']:.0f}" if pd.notna(row["RSI"]) else ""
                st.markdown(
                    f"**{row['Symbol']}** `${row['Price']:,.4f}`  \n"
                    f"1h: {c1h} | {rsi_str}  \n"
                    f"{tags_html}",
                    unsafe_allow_html=True,
                )
    else:
        st.info("No signals with current thresholds.")

    st.divider()

    # ── Table ──────────────────────────────────────────────────────────────────
    st.subheader(f"All pairs ({len(filtered)})")

    def fmt_signal(signals):
        if not signals:
            return "—"
        return " ".join(label for label, _ in signals)

    display = filtered.copy()
    display["Signals"] = display["Signals"].apply(fmt_signal)

    styled = (
        display.style
        .applymap(color_change, subset=["24h %", "1h %"])
        .applymap(color_rsi, subset=["RSI"])
        .format({
            "Price":   lambda v: f"${v:,.4f}" if pd.notna(v) else "—",
            "24h %":   lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "1h %":    lambda v: f"{v:+.2f}%" if pd.notna(v) else "—",
            "RSI":     lambda v: f"{v:.1f}" if pd.notna(v) else "—",
            "Vol x":   lambda v: f"{v:.2f}x" if pd.notna(v) else "—",
            "Fund %":  lambda v: f"{v:+.4f}%" if pd.notna(v) else "—",
        })
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)

    st.caption(
        f"Data from: {BASE / 'data'}  |  "
        f"Interval: {INTERVAL}  |  "
        f"Refreshing in {refresh_sec}s"
    )

time.sleep(refresh_sec)
st.rerun()
