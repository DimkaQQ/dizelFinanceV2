#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт нормализованного XLSX файла в базу данных DizelFinance.
Читает budget_normalized.xlsx и записывает все транзакции.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
import openpyxl
from datetime import datetime

# Твой Telegram ID
USER_ID = 612156666

def import_xlsx(filepath: str, dry_run: bool = True):
    """Импортирует транзакции из XLSX файла."""
    print(f"📂 Обработка: {filepath}")
    
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
    errors = 0
    
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
            
            # Форматируем дату (если уже в правильном формате, оставляем как есть)
            if not date_str:
                date_str = datetime.now().strftime("%d.%m.%Y")
            
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
                    "comment": "Импорт из budget_normalized.xlsx",
                    "source": "xlsx_import"
                })
            
            imported += 1
            
        except Exception as e:
            errors += 1
            if errors <= 5:  # Показываем только первые 5 ошибок
                print(f"   ⚠️  Ошибка в строке {row_idx}: {e}")
            continue
    
    if not dry_run:
        print(f"   ✅ Записано: {imported} | Пропущено: {skipped} | Ошибок: {errors}")
    
    return imported, skipped

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Импорт XLSX файла в базу данных")
    parser.add_argument("--file", default="./budget_normalized.xlsx", help="Путь к XLSX файлу")
    parser.add_argument("--dry-run", action="store_true", help="Тест без записи в БД")
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 РЕЖИМ ПРОСМОТРА. Данные НЕ будут записаны.\n")
    
    imported, skipped = import_xlsx(args.file, dry_run=args.dry_run)
    
    print(f"\n🎯 ИТОГО: {imported} транзакций {'готово к импорту' if args.dry_run else 'записано'}, {skipped} пропущено.")
    
    if not args.dry_run:
        print("💡 Данные сохранены в БД. Проверь дашборд/аналитику!")

if __name__ == "__main__":
    main()