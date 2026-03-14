"""
Pair filter system — extensible architecture.

Each filter is a dict:
    {
        "id":          str   — unique key
        "label":       str   — display name
        "description": str   — tooltip / explanation
        "column":      str   — DataFrame column to evaluate
        "mode":        "max" | "min" | "bool"
        "default":     float | bool
        "step":        float
        "range":       (min, max)
    }

To add a new filter — just append to FILTERS list.
"""

from __future__ import annotations
import pandas as pd

# ── Filter registry ─────────────────────────────────────────────────────────

FILTERS: list[dict] = [
    {
        "id":          "comm_ticks",
        "label":       "Макс. тиков комиссии",
        "description": "Сколько тиков нужно пройти чтобы отбить комиссию 0.1%. "
                       "Меньше — лучше. Рекомендуется ≤ 20.",
        "column":      "Comm ticks",
        "mode":        "max",
        "default":     50.0,
        "step":        1.0,
        "range":       (1.0, 200.0),
    },
    {
        "id":          "avg_range",
        "label":       "Мин. средний диапазон %",
        "description": "Минимальный средний диапазон свечи (high−low)/close×100. "
                       "Фильтрует слабоволатильные пары.",
        "column":      "Avg range %",
        "mode":        "min",
        "default":     0.0,
        "step":        0.05,
        "range":       (0.0, 5.0),
    },
    {
        "id":          "volume_24h",
        "label":       "Мин. объём 24ч (USDT)",
        "description": "Минимальный дневной объём в USDT. "
                       "Отсеивает неликвидные пары.",
        "column":      "Vol 24h",
        "mode":        "min",
        "default":     0.0,
        "step":        1_000_000.0,
        "range":       (0.0, 500_000_000.0),
    },
    # ── Добавляй новые фильтры сюда ────────────────────────────────────────
    # {
    #     "id":          "spread_pct",
    #     "label":       "Max Spread %",
    #     "description": "...",
    #     "column":      "Spread %",
    #     "mode":        "max",
    #     "default":     0.1,
    #     "step":        0.01,
    #     "range":       (0.0, 1.0),
    # },
]


# ── Apply filters ────────────────────────────────────────────────────────────

def apply_filters(df: pd.DataFrame, settings: dict[str, float]) -> pd.DataFrame:
    """
    Returns filtered DataFrame.
    settings: {filter_id: threshold_value}
    Columns missing from df are silently skipped.
    """
    mask = pd.Series(True, index=df.index)
    for f in FILTERS:
        fid = f["id"]
        col = f["column"]
        if fid not in settings or col not in df.columns:
            continue
        threshold = settings[fid]
        series = pd.to_numeric(df[col], errors="coerce")
        if f["mode"] == "max":
            # Pairs without tick data (None) are excluded — we can't verify they pass
            mask &= series.notna() & (series <= threshold)
        elif f["mode"] == "min":
            # skip pairs where value is NaN when threshold == default (0 / min)
            if threshold <= f["range"][0]:
                continue
            mask &= series.notna() & (series >= threshold)
    return df[mask]


def score_pair(row: pd.Series) -> float:
    """
    Composite score: lower is better.
    Returns NaN when Comm ticks is unknown (sorts to bottom).
    """
    if pd.isna(row.get("Comm ticks")):
        return float("nan")
    score = float(row["Comm ticks"]) * 2           # weight: most important
    if pd.notna(row.get("Avg range %")) and row["Avg range %"] > 0:
        score -= float(row["Avg range %"]) * 10    # higher range → lower score (better)
    return round(score, 2)
