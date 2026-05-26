import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _req(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise RuntimeError(f"Required env var {key!r} is not set")
    return val


@dataclass
class Config:
    # Telegram
    tg_api_id: int = field(default_factory=lambda: int(_req("TG_API_ID")))
    tg_api_hash: str = field(default_factory=lambda: _req("TG_API_HASH"))
    tg_channel: str = field(default_factory=lambda: _req("TG_CHANNEL"))

    # Claude
    anthropic_api_key: str = field(default_factory=lambda: _req("ANTHROPIC_API_KEY"))

    # Groq
    groq_api_key: str = field(default_factory=lambda: _req("GROQ_API_KEY"))

    # Bybit
    bybit_api_key: str = field(default_factory=lambda: _req("BYBIT_API_KEY"))
    bybit_api_secret: str = field(default_factory=lambda: _req("BYBIT_API_SECRET"))
    bybit_testnet: bool = field(
        default_factory=lambda: os.environ.get("BYBIT_TESTNET", "false").lower() == "true"
    )

    # Trading defaults
    default_leverage: int = field(
        default_factory=lambda: int(os.environ.get("DEFAULT_LEVERAGE", "10"))
    )
    default_size_usdt: float = field(
        default_factory=lambda: float(os.environ.get("DEFAULT_SIZE_USDT", "100"))
    )
    min_confidence: float = field(
        default_factory=lambda: float(os.environ.get("MIN_CONFIDENCE", "0.65"))
    )


config = Config()
