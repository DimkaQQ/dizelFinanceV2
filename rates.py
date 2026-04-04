# -*- coding: utf-8 -*-
import time
import logging
import requests
import xml.etree.ElementTree as ET
from config import FALLBACK_RATES

log = logging.getLogger(__name__)
_cache: dict = {}
_TTL = 3600

def get_rate(currency: str) -> float:
    if currency == "RUB": return 1.0
    cached = _cache.get(currency)
    if cached:
        rate, ts = cached
        if time.time() - ts < _TTL: return rate
    rate = _fetch(currency)
    _cache[currency] = (rate, time.time())
    return rate

def _fetch(currency: str) -> float:
    # 1. exchangerate-api
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/RUB", timeout=4)
        if r.status_code == 200:
            v = r.json()["rates"].get(currency)
            if v and v > 0: return round(1.0 / v, 6)
    except Exception as e:
        log.warning(f"exchangerate-api: {e}")
    # 2. cbr.ru
    try:
        r = requests.get("https://www.cbr.ru/scripts/XML_daily.asp", timeout=4)
        root = ET.fromstring(r.content)
        for val in root.findall("Valute"):
            if val.find("CharCode").text == currency:
                value   = val.find("Value").text.replace(",", ".")
                nominal = int(val.find("Nominal").text)
                return float(value) / nominal
    except Exception as e:
        log.warning(f"cbr.ru: {e}")
    # 3. open.er-api
    try:
        r = requests.get("https://open.er-api.com/v6/latest/RUB", timeout=4)
        if r.status_code == 200:
            v = r.json().get("rates", {}).get(currency)
            if v and v > 0: return round(1.0 / v, 6)
    except Exception as e:
        log.warning(f"open.er-api: {e}")
    fallback = FALLBACK_RATES.get(currency, 1.0)
    log.error(f"Все источники недоступны для {currency}, fallback: {fallback}")
    return fallback
