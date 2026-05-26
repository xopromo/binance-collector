import os
import tempfile

from groq import Groq

from ..config import config
from ..logger import get_logger

logger = get_logger(__name__)

_groq = Groq(api_key=config.groq_api_key)


def transcribe_audio(audio_bytes: bytes, filename: str = "audio.ogg") -> str:
    """Transcribe audio bytes using Groq Whisper."""
    ext = os.path.splitext(filename)[1] or ".ogg"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        with open(tmp_path, "rb") as f:
            result = _groq.audio.transcriptions.create(
                model="whisper-large-v3",
                file=(os.path.basename(tmp_path), f),
                response_format="text",
            )
        text = result if isinstance(result, str) else getattr(result, "text", str(result))
        logger.info(f"Audio transcribed: {len(text)} chars")
        return text
    finally:
        os.unlink(tmp_path)
