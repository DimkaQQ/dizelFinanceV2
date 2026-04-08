# -*- coding: utf-8 -*-
"""
Wealth Control — голосовые через faster-whisper (локально, бесплатно)
Установка: pip install faster-whisper
"""
import os
import logging
import tempfile

log = logging.getLogger(__name__)

# Размер модели: tiny/base/small/medium — чем больше тем точнее но медленнее
# На CPU без GPU рекомендуется: base или small
WHISPER_MODEL_SIZE = os.getenv("WHISPER_MODEL_SIZE", "base")
_model = None

def _get_model():
    global _model
    if _model is None:
        try:
            from faster_whisper import WhisperModel
            log.info(f"Загружаю Whisper модель: {WHISPER_MODEL_SIZE}")
            _model = WhisperModel(
                WHISPER_MODEL_SIZE,
                device="cpu",
                compute_type="int8"  # экономит память
            )
            log.info("✅ Whisper модель загружена")
        except ImportError:
            log.error("faster-whisper не установлен: pip install faster-whisper")
            return None
        except Exception as e:
            log.error(f"Ошибка загрузки Whisper: {e}")
            return None
    return _model


def transcribe(audio_bytes: bytes, filename: str = "voice.ogg") -> str | None:
    """
    Транскрибирует аудио через faster-whisper локально.
    Поддерживает: ogg, mp3, mp4, wav, webm, m4a
    """
    model = _get_model()
    if model is None:
        return None

    # Определяем расширение файла
    ext = "." + filename.split(".")[-1] if "." in filename else ".ogg"

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        segments, info = model.transcribe(
            tmp_path,
            language="ru",
            beam_size=5,
            vad_filter=True,  # убирает тишину
        )
        text = " ".join(s.text for s in segments).strip()
        log.info(f"Whisper: '{text[:80]}'")
        return text if text else None

    except Exception as e:
        log.error(f"transcribe: {e}")
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def transcribe_fallback(audio_bytes: bytes) -> str | None:
    """Фоллбэк — просто повторный вызов transcribe."""
    return transcribe(audio_bytes)
