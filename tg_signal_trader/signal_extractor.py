import json
import re
from dataclasses import dataclass, field
from typing import Optional

import anthropic

from .config import config
from .logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=config.anthropic_api_key)

_SYSTEM = """You are a professional crypto trading signal parser.
Extract trading signals from text and respond ONLY with a valid JSON object.

If a valid trading signal is present, respond:
{
  "valid": true,
  "symbol": "BTCUSDT",
  "direction": "LONG",
  "entry": 45000.0,
  "take_profits": [46000.0, 47000.0, 48000.0],
  "stop_loss": 44000.0,
  "leverage": 10,
  "size_usdt": null,
  "confidence": 0.9
}

If no valid trading signal found, respond:
{
  "valid": false,
  "reason": "brief explanation"
}

Rules:
- symbol: always UPPERCASE with USDT/BTC/ETH suffix, e.g. BTCUSDT, SOLUSDT, ETHUSDT
- direction: "LONG" or "SHORT" only
- entry: numeric price or null for market order
- take_profits: list of all TP levels in order
- stop_loss: numeric price, required for valid signal
- leverage: integer, default 10 if not mentioned
- size_usdt: null unless explicitly stated in the signal
- confidence: 0.0-1.0, reflects completeness and clarity of the signal
- A signal needs at minimum: symbol, direction, at least one TP, and stop_loss
- Do NOT invent values — only use what is explicitly stated"""


@dataclass
class Signal:
    valid: bool
    symbol: str = ""
    direction: str = ""
    entry: Optional[float] = None
    take_profits: list = field(default_factory=list)
    stop_loss: Optional[float] = None
    leverage: int = 10
    size_usdt: Optional[float] = None
    confidence: float = 0.0
    reason: str = ""
    raw_text: str = ""


def extract_signal(text: str) -> Signal:
    """Use Claude to parse a trading signal from combined text."""
    if not text or not text.strip():
        return Signal(valid=False, reason="Empty text")

    try:
        response = _client.messages.create(
            model="claude-opus-4-7",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            system=_SYSTEM,
            messages=[{"role": "user", "content": text[:8000]}],
        )
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return Signal(valid=False, reason=f"API error: {e}", raw_text=text)

    raw_response = ""
    for block in response.content:
        if block.type == "text":
            raw_response = block.text
            break

    # Pull the JSON out (Claude may wrap it in markdown)
    json_match = re.search(r"\{.*\}", raw_response, re.DOTALL)
    if not json_match:
        logger.warning(f"No JSON found in response: {raw_response[:300]}")
        return Signal(valid=False, reason="No JSON in response", raw_text=text)

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e} — raw: {raw_response[:300]}")
        return Signal(valid=False, reason=f"JSON parse error: {e}", raw_text=text)

    if not data.get("valid"):
        return Signal(
            valid=False,
            reason=data.get("reason", "No valid signal detected"),
            raw_text=text,
        )

    sig = Signal(
        valid=True,
        symbol=str(data.get("symbol", "")).upper().strip(),
        direction=str(data.get("direction", "LONG")).upper().strip(),
        entry=_to_float(data.get("entry")),
        take_profits=[float(x) for x in data.get("take_profits", []) if x],
        stop_loss=_to_float(data.get("stop_loss")),
        leverage=int(data.get("leverage") or config.default_leverage),
        size_usdt=_to_float(data.get("size_usdt")),
        confidence=float(data.get("confidence", 0.5)),
        raw_text=text,
    )

    logger.info(
        f"Signal: {sig.symbol} {sig.direction} | entry={sig.entry} | "
        f"TPs={sig.take_profits} | SL={sig.stop_loss} | "
        f"lev={sig.leverage}x | conf={sig.confidence:.2f}"
    )
    return sig


def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
