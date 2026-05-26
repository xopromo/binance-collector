import math
from typing import Optional

from pybit.unified_trading import HTTP

from .config import config
from .signal_extractor import Signal
from .logger import get_logger

logger = get_logger(__name__)

_session = HTTP(
    testnet=config.bybit_testnet,
    api_key=config.bybit_api_key,
    api_secret=config.bybit_api_secret,
)


def _get_qty_step(symbol: str) -> tuple[float, float]:
    """Return (qty_step, min_qty) for a linear symbol."""
    r = _session.get_instruments_info(category="linear", symbol=symbol)
    if r["retCode"] != 0:
        raise ValueError(f"get_instruments_info failed: {r['retMsg']}")
    lst = r["result"]["list"]
    if not lst:
        raise ValueError(f"Symbol not found: {symbol}")
    lot = lst[0]["lotSizeFilter"]
    return float(lot["qtyStep"]), float(lot["minOrderQty"])


def _get_mark_price(symbol: str) -> float:
    """Return current mark price for a symbol."""
    r = _session.get_tickers(category="linear", symbol=symbol)
    if r["retCode"] != 0:
        raise ValueError(f"get_tickers failed: {r['retMsg']}")
    return float(r["result"]["list"][0]["markPrice"])


def _round_qty(raw: float, step: float, minimum: float) -> str:
    """Round raw quantity down to step precision, enforce minimum."""
    decimals = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
    qty = math.floor(raw / step) * step
    qty = max(qty, minimum)
    return f"{qty:.{decimals}f}"


def _calc_qty(symbol: str, size_usdt: float, price: Optional[float]) -> str:
    ref_price = price or _get_mark_price(symbol)
    step, minimum = _get_qty_step(symbol)
    return _round_qty(size_usdt / ref_price, step, minimum)


def _set_leverage(symbol: str, leverage: int) -> None:
    r = _session.set_leverage(
        category="linear",
        symbol=symbol,
        buyLeverage=str(leverage),
        sellLeverage=str(leverage),
    )
    # retCode 110043 = leverage already set — not an error
    if r["retCode"] not in (0, 110043):
        raise RuntimeError(f"set_leverage failed: {r['retMsg']} ({r['retCode']})")
    logger.info(f"Leverage set to {leverage}x for {symbol}")


def execute_signal(signal: Signal) -> dict:
    """Open a USDT Perp position on Bybit based on a trading signal."""
    if not signal.valid:
        raise ValueError("Cannot execute an invalid signal")

    symbol = signal.symbol
    side = "Buy" if signal.direction == "LONG" else "Sell"
    size_usdt = signal.size_usdt or config.default_size_usdt
    leverage = signal.leverage or config.default_leverage

    logger.info(
        f"Placing {signal.direction} {symbol} | "
        f"leverage={leverage}x size={size_usdt} USDT"
    )

    _set_leverage(symbol, leverage)

    qty = _calc_qty(symbol, size_usdt, signal.entry)

    order: dict = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit" if signal.entry else "Market",
        "qty": qty,
        "positionIdx": 0,   # one-way mode
    }
    if signal.entry:
        order["price"] = str(signal.entry)
    if signal.stop_loss:
        order["stopLoss"] = str(signal.stop_loss)
        order["slTriggerBy"] = "MarkPrice"
    if signal.take_profits:
        order["takeProfit"] = str(signal.take_profits[0])
        order["tpTriggerBy"] = "MarkPrice"

    r = _session.place_order(**order)
    if r["retCode"] != 0:
        raise RuntimeError(f"place_order failed: {r['retMsg']} ({r['retCode']})")

    order_id = r["result"]["orderId"]
    logger.info(f"Order placed! id={order_id} {symbol} {side} qty={qty}")

    # Additional limit TP orders for TP2, TP3 …
    close_side = "Sell" if signal.direction == "LONG" else "Buy"
    if len(signal.take_profits) > 1:
        split_usdt = size_usdt / len(signal.take_profits)
        for i, tp in enumerate(signal.take_profits[1:], start=2):
            try:
                tp_qty = _calc_qty(symbol, split_usdt, signal.entry)
                tp_r = _session.place_order(
                    category="linear",
                    symbol=symbol,
                    side=close_side,
                    orderType="Limit",
                    qty=tp_qty,
                    price=str(tp),
                    positionIdx=0,
                    reduceOnly=True,
                )
                if tp_r["retCode"] == 0:
                    logger.info(f"TP{i} limit order placed at {tp}")
                else:
                    logger.warning(f"TP{i} order failed: {tp_r['retMsg']}")
            except Exception as e:
                logger.warning(f"TP{i} order error: {e}")

    return {
        "orderId": order_id,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "entry": signal.entry,
        "take_profits": signal.take_profits,
        "stop_loss": signal.stop_loss,
        "leverage": leverage,
    }
