import base64
import anthropic

from ..config import config
from ..logger import get_logger

logger = get_logger(__name__)

_client = anthropic.Anthropic(api_key=config.anthropic_api_key)

_PROMPT = (
    "This is a screenshot or image from a cryptocurrency trading signal channel. "
    "Extract and describe ALL trading-related content you see:\n"
    "- Coin/token symbol (e.g. BTC, ETH, SOL)\n"
    "- Direction: LONG or SHORT\n"
    "- Entry price(s)\n"
    "- Take Profit targets (TP1, TP2, TP3...)\n"
    "- Stop Loss level\n"
    "- Leverage (if mentioned)\n"
    "- Any chart patterns, trend lines, support/resistance levels\n"
    "- Any text, numbers, arrows, or annotations visible\n"
    "Be complete and precise — include every number you see."
)


def analyze_image(image_bytes: bytes, mime_type: str = "image/jpeg") -> str:
    """Analyze image with Claude vision, return text description."""
    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    response = _client.messages.create(
        model="claude-opus-4-7",
        max_tokens=2048,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": _PROMPT},
                ],
            }
        ],
    )
    text = ""
    for block in response.content:
        if block.type == "text":
            text = block.text
            break
    logger.info(f"Image analyzed: {len(text)} chars")
    return text
