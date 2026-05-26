from telethon.tl.types import (
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
)

from .config import config
from .logger import get_logger
from .media.audio import transcribe_audio
from .media.image import analyze_image
from .media.video import transcribe_video
from .signal_extractor import Signal, extract_signal
from .trader import execute_signal

logger = get_logger(__name__)


async def _get_media(message: Message) -> tuple[bytes | None, str]:
    """Download media and return (bytes, media_type)."""
    if not message.media:
        return None, "none"

    if isinstance(message.media, MessageMediaPhoto):
        data = await message.download_media(bytes)
        return data, "photo"

    if isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        mime: str = getattr(doc, "mime_type", "") or ""

        if mime.startswith("image/"):
            return await message.download_media(bytes), "image"

        if mime.startswith("video/") or mime == "video/mp4":
            return await message.download_media(bytes), "video"

        # Voice note = audio/ogg with OpusAudio attribute, or generic audio
        if mime.startswith("audio/") or "ogg" in mime:
            return await message.download_media(bytes), "audio"

    return None, "unknown"


async def process_message(message: Message) -> Signal | None:
    """Turn a Telegram message into a trading signal and execute it."""
    parts: list[str] = []

    if message.text:
        parts.append(message.text)

    media_bytes, media_type = await _get_media(message)

    if media_bytes and media_type not in ("none", "unknown"):
        try:
            if media_type in ("photo", "image"):
                logger.info(f"Analyzing image ({len(media_bytes):,} bytes)")
                parts.append("[IMAGE]\n" + analyze_image(media_bytes))

            elif media_type == "audio":
                logger.info(f"Transcribing audio ({len(media_bytes):,} bytes)")
                parts.append("[AUDIO]\n" + transcribe_audio(media_bytes))

            elif media_type == "video":
                logger.info(f"Transcribing video ({len(media_bytes):,} bytes)")
                parts.append("[VIDEO]\n" + transcribe_video(media_bytes))

        except Exception as exc:
            logger.error(f"Media processing failed [{media_type}]: {exc}")

    if not parts:
        logger.debug("Message has no processable content")
        return None

    combined = "\n\n".join(parts)
    signal = extract_signal(combined)

    if not signal.valid:
        logger.info(f"No signal found: {signal.reason}")
        return None

    if signal.confidence < config.min_confidence:
        logger.warning(
            f"Signal confidence {signal.confidence:.2f} < {config.min_confidence} — skipping"
        )
        return signal

    try:
        result = execute_signal(signal)
        logger.info(f"Trade executed: {result}")
    except Exception as exc:
        logger.error(f"Trade execution failed: {exc}", exc_info=True)

    return signal
