#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт истории из бюджетных CSV в DizelFinance.
Строго соблюдает категории из config.py. Новые НЕ создаёт.
"""
import sys
import os
import csv
import logging
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
from config import ALL_CATEGORIES, INCOME_CATEGORIES, BIG_EXPENSE_CATEGORIES, ASSET_CATEGORIES, MONTH_NAMES

# Настройка логгера
log = logging.getLogger(__name__)

# Твой Telegram ID
USER_ID = 408204060

# ══════════════════════════════════════════════════════════════════════════════
# 🔹 МАППИНГ: CSV-названия → КАНОНИЧЕСКИЕ КАТЕГОРИИ из config.py
# ══════════════════════════════════════════════════════════════════════════════
CSV_TO_CONFIG = {
    # Доходы
    "зарплата": "Зарплата",
    "консалтинг": "Консалтинг",
    "прочее": "Прочее (доход)",
    "займ": "Прочее (доход)",
    "терминал": "Прочее (доход)",
    "бомонд": "Прочее (доход)",
    "подарки": "Подарки (доход)",
    "дивидеднды": "Дивиденды",
    "дивиденды": "Дивиденды",

    # Регулярные расходы
    "ЕДА": "Продукты",
    "ПРОДУКТЫ": "Продукты",
    "продукты": "Продукты",
    "КОММУНАЛКА": "Аренда",
    "АЗС": "Такси",
    "ТРАНСПОРТ": "Такси",
    "Такси": "Такси",
    "ДЕТИ": "Ланочка",
    "Дети": "Ланочка",
    "ЛАНА (ЖЕНА)": "Ланочка",
    "Ланочка": "Ланочка",
    "МЕДИЦИНА": "Медицинские услуги",
    "Здоровье": "Медицинские услуги",
    "СОБАКА": "Джулиан",
    "Джулиан": "Джулиан",
    "ШТРАФЫ,НАЛОГИ": "Прочее (рег)",
    "Анечка": "Екатерина",
    "Екатерина": "Екатерина",
    "Влад": "Влад",
    "ГИГИЕНА,КРАСОТА": "Гигиена/Красота",
    "РЕСТОРАН ,КАФЕ": "Рестораны/кафе/фастфуд",
    "РЕСТОРАН": "Рестораны/кафе/фастфуд",
    "Кофе": "Рестораны/кафе/фастфуд",
    "Клубы, алкоголь": "Алкоголь",
    "АЛКАШКА": "Алкоголь",
    "ОДЕЖДА,ОБУВЬ": "Одежда/обувь/аксессуары",
    "ХОЗ.ТОВАРЫ": "Прочее (рег)",
    "БЫТ.ТОВАРЫ,ТЕХНИКА": "Прочее (рег)",
    "БЫТ.ТЕХНИКА,ГАДЖЕТ": "Гаджеты/техника",
    "Гаджет подписки": "Подписки",
    "РАЗВЛЕЧЕНИЯ": "Кино/театр/музеи",
    "Кино,театр,музеи": "Кино/театр/музеи",
    "Концерты": "Кино/театр/музеи",
    "СПОРТ": "Спорт",
    "ОБРАЗОВАНИЕ": "Образование (рег)",
    "ОБРАЗОВАНИЕ РАЗВИТИЕ": "Образование (рег)",
    "РЕЛАКС": "Релакс",
    "ПУТЕШЕСТВИЯ": "Путешествия (рег)",
    "Авиа/ржд": "Путешествия (рег)",
    "Отели": "Путешествия (рег)",
    "РАСХОД НА БИЗНЕС": "Прочее (рег)",
    "ОФИС": "Прочее (рег)",
    "Услуги": "Прочее (рег)",
    "Налоги штрафы комиссии": "Прочее (рег)",
    "БЛАГОТВОРИТЕЛЬНОСТЬ": "Прочее (рег)",
    "ПОДАРКИ": "Подарки (доход)",

    # Крупные траты
    "БЫТ.ТЕХНИКА,ГАДЖЕТ": "Гаджеты/техника",
    "Здоровье": "Здоровье",

    # Активы
    "ИНВЕСТИЦИИ": "Инвестиции в консалтинг",
    "ПОДУШКА": "Портфель Ланы",
    "КОПИЛКА НА ПУТЕШЕ": "Портфель Ланы",
    "Пенсионный план": "Пенсионный план",
    "Сбережения/обязательства": "Портфель Ланы",
    "Портфель Детей": "Портфель Екатерины",
}

ALLOWED_CATEGORIES = set(ALL_CATEGORIES.keys())

def normalize_category(raw_cat: str) -> str:
    """Жёстко маппит название из CSV в категорию config.py."""
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
    if not val or val.strip() in ["—", "", "0"]:
        return 0.0
    try:
        return float(val.replace(" ", "").replace("\xa0", "").replace(",", "."))
    except:
        return 0.0

def format_date_for_db(year: int, month: int, day: int = 15) -> str:
    """Возвращает дату в формате как в твоей БД: ДД.ММ.ГГГГ"""
    return f"{day:02d}.{month:02d}.{year}"

def check_duplicate(conn, user_id: int, date_str: str, category: str, amount: float) -> bool:
    """Проверяет дубликат, безопасно обрабатывая формат даты."""
    cur = conn.cursor()
    try:
        # Используем TO_DATE для безопасного сравнения
        cur.execute(
            """SELECT 1 FROM transactions 
               WHERE user_id=%s 
               AND TO_DATE(date, 'DD.MM.YYYY') = TO_DATE(%s, 'DD.MM.YYYY')
               AND category=%s 
               AND amount_rub=%s
               LIMIT 1""",
            (user_id, date_str, category, amount)
        )
        return cur.fetchone() is not None
    except Exception:
        # Если ошибка формата — считаем что не дубликат (чтобы не блокировать импорт)
        return False
    finally:
        cur.close()

def import_csv(filepath: str, year: int, dry_run: bool = True):
    print(f"📂 Обработка: {filepath} ({year})")
    if not os.path.exists(filepath):
        print("   ❌ Файл не найден!")
        return 0, 0

    imported, skipped, mapped_default = 0, 0, 0
    
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    for row in rows:
        if not row or len(row) < 3:
            continue

        raw_category = row[0].strip()
        if not raw_category or raw_category.upper() in ["КАТЕГОРИЯ", "ИТОГО", "ДЕЛЬТА", "ОБЩАЯ ТАБЛИЦА", "БЮДЖЕТ", "источник доходов"]:
            continue

        category = normalize_category(raw_category)
        
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

        fact_indices = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]
        for idx in fact_indices:
            if idx >= len(row):
                break
                
            amount = clean_number(row[idx])
            if amount <= 0:
                continue

            month_num = int((idx - 2) / 2) + 1
            if not (1 <= month_num <= 12):
                continue

            date_str = format_date_for_db(year, month_num)
            
            # Проверка дубликатов — ТОЛЬКО если не dry-run
            is_duplicate = False
            if not dry_run:
                try:
                    conn = db.get_conn()
                    is_duplicate = check_duplicate(conn, USER_ID, date_str, category, amount)
                    conn.close()
                except Exception as e:
                    log.warning(f"DB check warning: {e}")
                    is_duplicate = False  # Не блокируем импорт при ошибке
            
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
            if category == "Прочее (рег)" and raw_category not in ["Прочее (рег)", "ПРОЧЕЕ (РЕГ)", "ХОЗ.ТОВАРЫ", "РАСХОД НА БИЗНЕС", "ОФИС", "Услуги", "БЛАГОТВОРИТЕЛЬНОСТЬ", "ШТРАФЫ,НАЛОГИ"]:
                mapped_default += 1

    if not dry_run:
        print(f"   ✅ Записано: {imported} | Пропущено дублей: {skipped} | Ушло в 'Прочее': {mapped_default}")
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