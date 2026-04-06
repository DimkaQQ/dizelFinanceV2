# -*- coding: utf-8 -*-
"""
DizelFinance — парсинг TXT файлов (банковские выписки, экспорты)
"""

import logging
import json
from ai import ask_claude, extract_json

log = logging.getLogger(__name__)


def parse_txt(content: str) -> list[dict]:
    """
    Парсит текстовый файл с транзакциями через Claude.
    Поддерживает: выписки Тинькофф txt, Сбер txt, произвольные форматы.
    """
    if not content or len(content.strip()) < 10:
        return []

    # Ограничиваем размер — Claude принимает до ~200k токенов
    content = content[:50000]

    prompt = (
        "Это текстовый файл с банковскими транзакциями или финансовыми данными.\n"
        "Извлеки ВСЕ транзакции которые найдёшь.\n\n"
        "Для каждой транзакции верни:\n"
        "- date: ДД.ММ.ГГГГ\n"
        "- amount: число (положительное)\n"
        "- currency: RUB/USD/EUR/KZT (определи из контекста, по умолчанию RUB)\n"
        "- merchant: название места/получателя\n"
        "- tx_type: 'Расход' или 'Доход'\n"
        "- category_hint: категория если указана, иначе ''\n\n"
        "Игнорируй: технические строки, заголовки, балансы, итоговые суммы.\n"
        "Ответь ТОЛЬКО JSON массивом. Если транзакций нет — [].\n\n"
        f"Содержимое файла:\n{content}"
    )

    try:
        result = ask_claude(prompt)
        data   = extract_json(result)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []
    except Exception as e:
        log.error(f"parse_txt: {e}")
        return []


def detect_encoding(raw_bytes: bytes) -> str:
    """Определяет кодировку файла."""
    encodings = ["utf-8", "cp1251", "utf-16", "latin-1"]
    for enc in encodings:
        try:
            raw_bytes.decode(enc)
            return enc
        except (UnicodeDecodeError, ValueError):
            continue
    return "utf-8"


def read_txt_file(raw_bytes: bytes) -> str:
    """Читает TXT файл с автоопределением кодировки."""
    enc = detect_encoding(raw_bytes)
    try:
        return raw_bytes.decode(enc, errors="replace")
    except Exception:
        return raw_bytes.decode("utf-8", errors="replace")
