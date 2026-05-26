"""
TG Signal Trader — entry point.

Usage:
    python -m tg_signal_trader
"""

import asyncio
import sys

from .listener import run
from .logger import get_logger

logger = get_logger("tg_signal_trader")


def main() -> None:
    logger.info("=" * 50)
    logger.info("TG Signal Trader starting...")
    logger.info("=" * 50)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as exc:
        logger.error(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
