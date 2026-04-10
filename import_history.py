#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт истории из бюджетных файлов в базу данных DizelFinance.
Поддерживает:
1. CSV файлы с бюджетами (2023, 2024, 2025)
2. Простые XLSX файлы (budget_simple.xlsx)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
import openpyxl
import csv
from datetime import datetime

# Твой Telegram ID
USER_ID = 612156666

def import_simple_xlsx(filepath: str, dry_run: bool = True):
    """Импортирует транзакции из простого XLSX файла (budget_simple.xlsx)."""
    print(f"📂 Обработка простого XLSX: {filepath}")
    
    if not os.path.exists(filepath):
        print(f"   ❌ Файл не найден: {filepath}")
        return 0, 0
    
    try:
        wb = openpyxl.load_workbook(filepath)
        ws = wb.active
    except Exception as e:
        print(f"   ❌ Ошибка открытия файла: {e}")
        return 0, 0
    
    imported = 0
    skipped = 0
    
    # Пропускаем заголовок (первая строка)
    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx == 1:  # Заголовок
            continue
        
        if not row or all(cell is None for cell in row):
            continue
        
        try:
            # Ожидаемый формат: date, amount, currency, merchant, tx_type, category
            date_str = str(row[0]).strip() if row[0] else ""
            amount = float(row[1]) if row[1] else 0.0
            currency = str(row[2]).strip() if row[2] else "RUB"
            merchant = str(row[3]).strip() if row[3] else "Импорт"
            tx_type = str(row[4]).strip() if row[4] else "Расход"
            category = str(row[5]).strip() if row[5] else "Прочее (рег)"
            
            # Валидация
            if amount <= 0:
                skipped += 1
                continue
            
            if tx_type not in ["Расход", "Доход", "Актив"]:
                tx_type = "Расход"
            
            # Определяем раздел
            section = "Регулярные расходы"
            if tx_type == "Доход":
                section = "Доходы"
            elif tx_type == "Актив":
                section = "Движение активов"
            
            if dry_run:
                print(f"   [+] {date_str} | {tx_type:6} | {category:30} | {amount:10,.0f} ₽")
            else:
                db.save_transaction(USER_ID, {
                    "date": date_str,
                    "section": section,
                    "category": category,
                    "amount": amount,
                    "currency": currency,
                    "rate": 1.0,
                    "amount_rub": amount,
                    "tx_type": tx_type,
                    "merchant": merchant,
                    "comment": "Импорт из budget_simple.xlsx",
                    "source": "xlsx_simple_import"
                })
            
            imported += 1
            
        except Exception as e:
            print(f"   ⚠️  Ошибка в строке {row_idx}: {e}")
            skipped += 1
            continue
    
    if not dry_run:
        print(f"   ✅ Записано: {imported} | Пропущено: {skipped}")
    
    return imported, skipped

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Импорт бюджетных файлов в базу данных")
    parser.add_argument("--dry-run", action="store_true", help="Тест без записи в БД")
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА. Данные НЕ будут записаны.\n")
    
    total_imported = 0
    total_skipped = 0
    
    # 1. Импортируем budget_simple.xlsx
    simple_file = "./budget_simple.xlsx"
    if os.path.exists(simple_file):
        imp, skip = import_simple_xlsx(simple_file, dry_run=args.dry_run)
        total_imported += imp
        total_skipped += skip
        print()
    
    # 2. Импортируем CSV файлы бюджетов (если нужно)
    # Можешь раскомментировать если нужно импортировать и CSV тоже
    """
    csv_files = [
        ("./Мой учет дох_расх.xlsx - Бюджет 2023.csv", 2023),
        ("./Мой учет дох_расх.xlsx - Бюджет 24.csv", 2024),
        ("./Мой_учет_дох_расх_xlsx_Бюджет_2025_1.csv", 2025),
    ]
    
    for filepath, year in csv_files:
        if os.path.exists(filepath):
            # Здесь будет логика импорта CSV (как в предыдущей версии)
            pass
    """
    
    print(f"\n🎯 ИТОГО: {total_imported} транзакций {'готово к импорту' if args.dry_run else 'записано'}, {total_skipped} пропущено.")
    
    if not args.dry_run:
        print("💡 Данные сохранены в БД. Проверь дашборд/аналитику!")

if __name__ == "__main__":
    main()