#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт истории из бюджетных CSV в DizelFinance.
Поддерживает файлы с структурой: план/факт И план/факт/отклонение.
"""
import sys
import os
import csv
import logging
import re
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
from config import ALL_CATEGORIES, INCOME_CATEGORIES, BIG_EXPENSE_CATEGORIES, ASSET_CATEGORIES, MONTH_NAMES

log = logging.getLogger(__name__)
USER_ID = 408204060  # Твой Telegram ID

# ══════════════════════════════════════════════════════════════════════════════
# 🔹 МАППИНГ: названия из CSV → канонические категории из config.py
# ══════════════════════════════════════════════════════════════════════════════
CSV_TO_CONFIG = {
    # Доходы
    "зарплата": "Зарплата", "консалтинг": "Консалтинг", "прочее": "Прочее (доход)",
    "займ": "Прочее (доход)", "терминал": "Прочее (доход)", "бомонд": "Прочее (доход)",
    "подарки": "Подарки (доход)", "дивидеднды": "Дивиденды", "дивиденды": "Дивиденды",
    
    # Регулярные расходы
    "ЕДА": "Продукты", "ПРОДУКТЫ": "Продукты", "продукты": "Продукты",
    "КОММУНАЛКА": "Аренда", "АЗС": "Такси", "ТРАНСПОРТ": "Такси", "Такси": "Такси",
    "ДЕТИ": "Ланочка", "Дети": "Ланочка", "ЛАНА (ЖЕНА)": "Ланочка", "Ланочка": "Ланочка",
    "МЕДИЦИНА": "Медицинские услуги", "Здоровье": "Медицинские услуги",
    "СОБАКА": "Джулиан", "Джулиан": "Джулиан",
    "ШТРАФЫ,НАЛОГИ": "Прочее (рег)", "Анечка": "Екатерина", "Екатерина": "Екатерина", "Влад": "Влад",
    "ГИГИЕНА,КРАСОТА": "Гигиена/Красота",
    "РЕСТОРАН ,КАФЕ": "Рестораны/кафе/фастфуд", "РЕСТОРАН": "Рестораны/кафе/фастфуд", "Кофе": "Рестораны/кафе/фастфуд",
    "Клубы, алкоголь": "Алкоголь", "АЛКАШКА": "Алкоголь",
    "ОДЕЖДА,ОБУВЬ": "Одежда/обувь/аксессуары",
    "ХОЗ.ТОВАРЫ": "Прочее (рег)", "БЫТ.ТОВАРЫ,ТЕХНИКА": "Прочее (рег)",
    "БЫТ.ТЕХНИКА,ГАДЖЕТ": "Гаджеты/техника", "Гаджет подписки": "Подписки",
    "РАЗВЛЕЧЕНИЯ": "Кино/театр/музеи", "Кино,театр,музеи": "Кино/театр/музеи", "Концерты": "Кино/театр/музеи",
    "СПОРТ": "Спорт", "ОБРАЗОВАНИЕ": "Образование (рег)", "ОБРАЗОВАНИЕ РАЗВИТИЕ": "Образование (рег)",
    "РЕЛАКС": "Релакс", "ПУТЕШЕСТВИЯ": "Путешествия (рег)", "Авиа/ржд": "Путешествия (рег)", "Отели": "Путешествия (рег)",
    "РАСХОД НА БИЗНЕС": "Прочее (рег)", "ОФИС": "Прочее (рег)", "Услуги": "Прочее (рег)",
    "Налоги штрафы комиссии": "Прочее (рег)", "БЛАГОТВОРИТЕЛЬНОСТЬ": "Прочее (рег)",
    
    # Активы
    "ИНВЕСТИЦИИ": "Инвестиции в консалтинг", "ПОДУШКА": "Портфель Ланы",
    "КОПИЛКА НА ПУТЕШЕ": "Портфель Ланы", "Пенсионный план": "Пенсионный план",
    "Сбережения/обязательства": "Портфель Ланы", "Портфель Детей": "Портфель Екатерины",
}

ALLOWED_CATEGORIES = set(ALL_CATEGORIES.keys())
MONTHS_RU = ["январь", "февраль", "март", "апрель", "май", "июнь", 
             "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]

def normalize_category(raw_cat: str) -> str:
    """Маппит название из CSV в категорию config.py."""
    cleaned = raw_cat.strip().upper()
    if cleaned in CSV_TO_CONFIG:
        return CSV_TO_CONFIG[cleaned]
    for key, val in CSV_TO_CONFIG.items():
        if key in cleaned:
            return val
    if cleaned in ALLOWED_CATEGORIES:
        return cleaned
    return "Прочее (рег)"

def clean_number(val: str) -> float:
    """Очистка числа из CSV."""
    if not val or val.strip() in ["—", "", "0"]:
        return 0.0
    try:
        return float(val.replace(" ", "").replace("\xa0", "").replace(",", "."))
    except:
        return 0.0

def parse_fact_columns(header_row: list) -> dict[int, int]:
    """
    Авто-определение индексов столбцов с ФАКТИЧЕСКИМИ значениями.
    Возвращает: {номер_месяца: индекс_колонки_с_фактом}
    """
    month_map = {}
    
    for i, cell in enumerate(header_row):
        if not cell:
            continue
        cell_lower = cell.strip().lower()
        
        # Ищем названия месяцев
        for m_num, month_name in enumerate(MONTHS_RU, 1):
            if month_name in cell_lower:
                # Ищем "факт" в следующих 1-2 колонках
                for offset in [1, 2]:
                    if i + offset < len(header_row):
                        next_cell = (header_row[i + offset] or "").strip().lower()
                        if "факт" in next_cell:
                            month_map[m_num] = i + offset
                            break
                break
    return month_map

def is_data_row(row: list) -> bool:
    """Проверяет, является ли строка данными, а не заголовком."""
    if not row or len(row) < 3:
        return False
    first = (row[0] or "").strip().upper()
    
    skip_keywords = [
        "КАТЕГОРИЯ", "ИТОГО", "ДЕЛЬТА", "ОБЩАЯ ТАБЛИЦА", "БЮДЖЕТ", 
        "ИСТОЧНИК ДОХОДОВ", "СТАТЬЯ РАСХОДОВ", "ВАЛЮТА", "РАСХОД", 
        "ДОХОД", "ЖИЗНЬ", "ДВИЖЕНИЕ АКТИВ", "ПОДПИСКИ", "КРЕДИТ",
        "СТРАХОВКА", "АЛЬФА", "СБЕР", "ПОРТФЕЛЬ", "ПЕНСИОННЫЙ",
        "ПЛАН", "ФАКТ", "ОТКЛОНЕНИЕ", "НА СЕГОДНЯ", "ОСТАТОК"
    ]
    if any(kw in first for kw in skip_keywords):
        return False
    
    # Проверяем, есть ли числовые значения в строке
    has_number = any(re.match(r'[\d\s,]+', str(cell).strip()) for cell in row[2:] if cell)
    return bool(first and has_number)

def import_csv(filepath: str, year: int, dry_run: bool = True):
    print(f"📂 Обработка: {filepath} ({year})")
    if not os.path.exists(filepath):
        print("   ❌ Файл не найден!")
        return 0, 0

    imported, skipped = 0, 0
    
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # 1. Находим заголовок с месяцами
    month_indices = {}
    for i, row in enumerate(rows):
        if any(month_name in str(c).lower() for c in (row or []) for month_name in MONTHS_RU):
            month_indices = parse_fact_columns(row)
            if month_indices:
                print(f"   📅 Найдено месяцев: {len(month_indices)} -> {month_indices}")
                break
    
    if not month_indices:
        print("   ⚠️  Не найдены колонки с месяцами, пропускаем файл")
        return 0, 0

    # 2. Обрабатываем строки с данными
    for row in rows:
        if not is_data_row(row):
            continue

        raw_category = (row[0] or "").strip()
        category = normalize_category(raw_category)
        
        # Определяем тип и раздел
        tx_type = "Расход"
        section = "Регулярные расходы"
        if category in INCOME_CATEGORIES:
            tx_type = "Доход"
            section = "Доходы"
        elif category in BIG_EXPENSE_CATEGORIES:
            section = "Крупные траты"
        elif category in ASSET_CATEGORIES:
            tx_type = "Актив"
            section = "Движение активов"

        # 3. Проходим по найденным месяцам и извлекаем фактические значения
        for month_num, fact_idx in month_indices.items():
            if fact_idx >= len(row):
                continue
                
            amount = clean_number(row[fact_idx])
            if amount <= 0:
                continue

            date_str = f"15.{month_num:02d}.{year}"
            
            # Проверка дубликатов (только если не dry-run)
            is_duplicate = False
            if not dry_run:
                try:
                    conn = db.get_conn()
                    cur = conn.cursor()
                    cur.execute(
                        """SELECT 1 FROM transactions 
                           WHERE user_id=%s AND date=%s AND category=%s AND amount_rub=%s LIMIT 1""",
                        (USER_ID, date_str, category, amount)
                    )
                    is_duplicate = cur.fetchone() is not None
                    cur.close()
                    conn.close()
                except Exception as e:
                    log.warning(f"DB check warning: {e}")
            
            if is_duplicate:
                skipped += 1
                continue

            if dry_run:
                print(f"   [+] {date_str} | {tx_type:6} | {category:30} | {amount:10,.0f} ₽")
            else:
                db.save_transaction(USER_ID, {
                    "date": date_str,
                    "section": section,
                    "category": category,
                    "amount": amount,
                    "currency": "RUB",
                    "rate": 1.0,
                    "amount_rub": amount,
                    "tx_type": tx_type,
                    "merchant": f"Импорт бюджета {year}",
                    "comment": f"Агрегированные данные за {MONTH_NAMES.get(month_num, '')} {year}",
                    "source": "budget_import"
                })
            
            imported += 1

    if not dry_run:
        print(f"   ✅ Записано: {imported} | Пропущено дублей: {skipped}")
    return imported, skipped

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Тест без записи в БД")
    parser.add_argument("--files", nargs="+", help="Пути к CSV")
    args = parser.parse_args()

    files = args.files or [
        ("./Мой учет дох_расх.xlsx - Бюджет 2023.csv", 2023),
        ("./Мой учет дох_расх.xlsx - Бюджет 24.csv", 2024),
        ("./Мой_учет_дох_расх_xlsx_Бюджет_2025_1.csv", 2025),
    ]

    if args.dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА. Данные НЕ будут записаны.\n")
        
    total_imp, total_skip = 0, 0
    for item in files:
        path, year = item if isinstance(item, tuple) else (item, 2024)
        imp, skip = import_csv(path, year, dry_run=args.dry_run)
        total_imp += imp
        total_skip += skip

    print(f"\n🎯 ИТОГО: {total_imp} транзакций готово к импорту, {total_skip} дублей пропущено.")
    if not args.dry_run:
        print("💡 Данные сохранены в БД. Проверь дашборд/аналитику!")

if __name__ == "__main__":
    main()