#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт истории транзакций в DizelFinance из budget_import.xlsx
Запуск:
    python import_history.py            — dry-run (только просмотр)
    python import_history.py --save     — реальная запись в БД
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import openpyxl
from datetime import datetime

# ─── Настройки ────────────────────────────────────────────────────────────────
USER_ID   = 612156666
XLSX_FILE = "./budget_import.xlsx"

# Категории которые идут в Крупные траты (остальные расходы → Регулярные расходы)
КРУПНЫЕ_ТРАТЫ = {
    'Путешествия (рег)',
    'Гаджеты/техника',
    'Екатерина',
    'Ланочка',
}
# ──────────────────────────────────────────────────────────────────────────────


def get_section(tx_type: str, category: str) -> str:
    if tx_type == 'Доход':
        return 'Доходы'
    if tx_type == 'Актив':
        return 'Движение активов'
    if category in КРУПНЫЕ_ТРАТЫ:
        return 'Крупные траты'
    return 'Регулярные расходы'


def parse_date(date_str: str):
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Не могу распарсить дату: {date_str}")


def load_transactions_from_xlsx(filepath: str) -> list[dict]:
    """Читает все транзакции из xlsx файла."""
    if not os.path.exists(filepath):
        print(f"❌ Файл не найден: {filepath}")
        sys.exit(1)

    wb = openpyxl.load_workbook(filepath)
    ws = wb['Транзакции']
    rows = list(ws.iter_rows(values_only=True))

    transactions = []
    errors = []

    for row_idx, row in enumerate(rows[1:], start=2):  # skip header
        if not row or all(v is None for v in row):
            continue
        try:
            date_str  = str(row[0]).strip()
            amount    = float(row[1])
            currency  = str(row[2]).strip() if row[2] else 'RUB'
            merchant  = str(row[3]).strip() if row[3] else 'Импорт'
            tx_type   = str(row[4]).strip() if row[4] else 'Расход'
            category  = str(row[5]).strip() if row[5] else 'Прочее (рег)'

            if amount <= 0:
                errors.append(f"  Строка {row_idx}: сумма = {amount}, пропущено")
                continue

            if tx_type not in ('Доход', 'Расход', 'Актив'):
                errors.append(f"  Строка {row_idx}: неизвестный tx_type '{tx_type}', ставлю Расход")
                tx_type = 'Расход'

            date = parse_date(date_str)
            section = get_section(tx_type, category)

            transactions.append({
                'date':       date.strftime('%d.%m.%Y'),
                'amount':     amount,
                'currency':   currency,
                'rate':       1.0,
                'amount_rub': amount,
                'merchant':   merchant,
                'tx_type':    tx_type,
                'section':    section,
                'category':   category,
                'comment':    'Импорт истории',
                'source':     'xlsx_import',
            })

        except Exception as e:
            errors.append(f"  Строка {row_idx}: {e}")

    return transactions, errors


def print_summary(transactions: list[dict]):
    """Выводит красивую сводку по транзакциям."""
    from collections import defaultdict

    by_year_type = defaultdict(lambda: {'cnt': 0, 'sum': 0})
    for tx in transactions:
        year = tx['date'].split('.')[-1]
        key = (year, tx['tx_type'])
        by_year_type[key]['cnt'] += 1
        by_year_type[key]['sum'] += tx['amount']

    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│              СВОДКА ПО ТРАНЗАКЦИЯМ                     │")
    print("├──────┬──────────┬────────────┬────────────────────────-┤")
    print("│  Год │   Тип    │    Кол-во  │          Сумма          │")
    print("├──────┼──────────┼────────────┼─────────────────────────┤")

    for (year, tx_type) in sorted(by_year_type.keys()):
        d = by_year_type[(year, tx_type)]
        print(f"│ {year} │ {tx_type:<8} │ {d['cnt']:>10} │ {d['sum']:>21,.0f} ₽ │")

    print("├──────┴──────────┴────────────┴─────────────────────────┤")
    print(f"│  Всего транзакций: {len(transactions):<37}│")
    print("└─────────────────────────────────────────────────────────┘\n")


def dry_run(transactions: list[dict]):
    """Показывает что будет импортировано без записи в БД."""
    print("\n🔍 DRY-RUN — данные НЕ записываются в БД\n")
    print(f"{'#':<5} {'Дата':<12} {'Тип':<8} {'Раздел':<22} {'Категория':<28} {'Сумма':>12}")
    print("─" * 95)

    for i, tx in enumerate(transactions, 1):
        print(
            f"{i:<5} {tx['date']:<12} {tx['tx_type']:<8} "
            f"{tx['section']:<22} {tx['category']:<28} "
            f"{tx['amount']:>10,.0f} ₽"
        )

    print_summary(transactions)


def save_to_db(transactions: list[dict]):
    """Записывает транзакции в БД пакетно."""
    try:
        import database as db
    except ImportError:
        print("❌ Не могу импортировать database.py — убедись что скрипт в папке проекта")
        sys.exit(1)

    print(f"\n💾 Записываю {len(transactions)} транзакций в БД...")

    saved = db.save_transactions_batch(USER_ID, transactions)
    print(f"✅ Записано: {saved} транзакций")
    print("💡 Проверь дашборд и аналитику!")


def main():
    dry = '--save' not in sys.argv

    print(f"📂 Читаю файл: {XLSX_FILE}")
    transactions, errors = load_transactions_from_xlsx(XLSX_FILE)

    if errors:
        print(f"\n⚠️  Ошибки при парсинге ({len(errors)} шт.):")
        for e in errors:
            print(e)

    if not transactions:
        print("❌ Нет транзакций для импорта")
        sys.exit(1)

    if dry:
        dry_run(transactions)
        print("─" * 60)
        print("➡️  Запусти с флагом --save чтобы записать в БД:")
        print("    python import_history.py --save")
    else:
        print_summary(transactions)
        save_to_db(transactions)


if __name__ == '__main__':
    main()