import os
import subprocess
import tempfile

from .audio import transcribe_audio
from ..logger import get_logger

logger = get_logger(__name__)


def _extract_audio(video_path: str, audio_path: str) -> None:
    """Extract audio track from video file using ffmpeg."""
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", video_path,
            "-vn",                  # no video
            "-acodec", "libmp3lame",
            "-q:a", "4",
            audio_path,
        ],
        check=True,
        capture_output=True,
    )


def transcribe_video(video_bytes: bytes) -> str:
    """Extract audio from video and transcribe with Groq Whisper."""
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp_v:
        tmp_v.write(video_bytes)
        video_path = tmp_v.name

    audio_path = video_path.replace(".mp4", ".mp3")

    try:
        logger.info(f"Extracting audio from video ({len(video_bytes)} bytes)...")
        _extract_audio(video_path, audio_path)

        with open(audio_path, "rb") as f:
            audio_bytes = f.read()

        return transcribe_audio(audio_bytes, "video_audio.mp3")
    finally:
        for path in (video_path, audio_path):
            if os.path.exists(path):
                os.unlink(path)
