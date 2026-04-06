# -*- coding: utf-8 -*-
"""
DizelFinance — голосовые сообщения через Groq Whisper
"""

import os
import logging
import requests
import tempfile

log = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "whisper-large-v3-turbo"  # быстрый, русский хорошо
GROQ_URL     = "https://api.groq.com/openai/v1/audio/transcriptions"


def transcribe(audio_bytes: bytes, filename: str = "voice.ogg") -> str | None:
    """
    Отправляет аудио в Groq Whisper, возвращает текст транскрипции.
    Поддерживает: ogg, mp3, mp4, wav, webm, m4a
    """
    if not GROQ_API_KEY:
        log.error("GROQ_API_KEY не задан в .env")
        return None

    try:
        response = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            files={"file": (filename, audio_bytes, "audio/ogg")},
            data={
                "model":       GROQ_MODEL,
                "language":    "ru",
                "temperature": "0",
                "response_format": "text",
            },
            timeout=30,
        )

        if response.status_code == 200:
            text = response.text.strip()
            log.info(f"Whisper транскрипция: {text[:100]}")
            return text
        else:
            log.error(f"Groq Whisper error {response.status_code}: {response.text[:200]}")
            return None

    except Exception as e:
        log.error(f"transcribe exception: {e}")
        return None


def transcribe_fallback(audio_bytes: bytes) -> str | None:
    """
    Фоллбэк на Faster-Whisper если Groq недоступен.
    Требует: pip install faster-whisper
    """
    try:
        from faster_whisper import WhisperModel
        import io

        model = WhisperModel("small", device="cpu", compute_type="int8")
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        segments, _ = model.transcribe(tmp_path, language="ru")
        text = " ".join(s.text for s in segments).strip()
        os.unlink(tmp_path)
        return text if text else None

    except ImportError:
        log.warning("faster-whisper не установлен")
        return None
    except Exception as e:
        log.error(f"faster-whisper: {e}")
        return None
