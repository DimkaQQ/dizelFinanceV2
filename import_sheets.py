# -*- coding: utf-8 -*-
"""
DizelFinance — импорт истории из Google Sheets в PostgreSQL
Запуск: python import_sheets.py --user_id=123456789 --year=2024

Читает листы ЯНВАРЬ, ФЕВРАЛЬ... из Planergo таблицы,
нормализует категории через CANONICAL_MAP, пишет в PostgreSQL.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
from config import CANONICAL_MAP, ALL_CATEGORIES, MONTH_NAMES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Google Sheets подключение ─────────────────────────────────────────────────
SHEET_URL = os.getenv("SHEET_URL")
KEY_FILE  = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "finance-key.json")

MONTH_SHEET_NAMES = {
    "ЯНВАРЬ":   1,  "ФЕВРАЛЬ":  2,  "МАРТ":     3,  "АПРЕЛЬ": 4,
    "МАЙ":      5,  "ИЮНЬ":     6,  "ИЮЛЬ":     7,  "АВГУСТ": 8,
    "СЕНТЯБРЬ": 9,  "ОКТЯБРЬ": 10,  "НОЯБРЬ":  11,  "ДЕКАБРЬ":12,
}

# Строки где начинаются данные транзакций в месячных листах
TX_START_ROW = 46

# Колонки для доходов (Поступления / Движение активов)
INCOME_DATE_COL   = 2   # B
INCOME_ART_COL    = 5   # E
INCOME_AMOUNT_COL = 16  # P

# Колонки для расходов (Крупные расходы / Расходы)
EXPENSE_DATE_COL   = 35  # AI
EXPENSE_ART_COL    = 38  # AL
EXPENSE_AMOUNT_COL = 49  # AW


def normalize_category(old_cat: str) -> tuple[str, str]:
    """Маппинг старой категории → новая категория + раздел."""
    old_cat = old_cat.strip()

    # Прямое совпадение в новых категориях
    if old_cat in ALL_CATEGORIES:
        return old_cat, ALL_CATEGORIES[old_cat]

    # Через CANONICAL_MAP
    new_cat = CANONICAL_MAP.get(old_cat)
    if new_cat and new_cat in ALL_CATEGORIES:
        return new_cat, ALL_CATEGORIES[new_cat]

    # Фоллбэк
    log.warning(f"Неизвестная категория: '{old_cat}' → Прочее (рег)")
    return "Прочее (рег)", "Регулярные расходы"


def parse_amount(val) -> float:
    """Парсит сумму из ячейки."""
    if not val:
        return 0.0
    try:
        return float(str(val).replace(" ", "").replace(",", ".").replace("₽", "").strip())
    except ValueError:
        return 0.0


def import_month_sheet(ws, year: int, month: int, user_id: int) -> int:
    """Читает один месячный лист и записывает транзакции в PostgreSQL."""
    all_values = ws.get_all_values()
    count = 0

    for row_idx in range(TX_START_ROW - 1, len(all_values)):
        row = all_values[row_idx]

        def cell(col_idx):
            return row[col_idx - 1] if col_idx <= len(row) else ""

        # ── Доходы ──
        inc_date = cell(INCOME_DATE_COL).strip()
        inc_art  = cell(INCOME_ART_COL).strip()
        inc_amt  = parse_amount(cell(INCOME_AMOUNT_COL))

        if inc_date and inc_art and inc_amt > 0:
            cat, sec = normalize_category(inc_art)
            try:
                date_obj = datetime.strptime(inc_date, "%d.%m.%Y").date()
            except ValueError:
                date_obj = datetime(year, month, 1).date()

            db.save_transaction(user_id, {
                "date":       date_obj.strftime("%d.%m.%Y"),
                "section":    sec,
                "category":   cat,
                "amount":     inc_amt,
                "currency":   "RUB",
                "rate":       1.0,
                "amount_rub": inc_amt,
                "tx_type":    "Доход",
                "merchant":   "",
                "comment":    f"Импорт из Sheets {year}",
                "source":     "sheets_import",
            })
            count += 1

        # ── Расходы ──
        exp_date = cell(EXPENSE_DATE_COL).strip()
        exp_art  = cell(EXPENSE_ART_COL).strip()
        exp_amt  = parse_amount(cell(EXPENSE_AMOUNT_COL))

        if exp_date and exp_art and exp_amt > 0:
            cat, sec = normalize_category(exp_art)
            try:
                date_obj = datetime.strptime(exp_date, "%d.%m.%Y").date()
            except ValueError:
                date_obj = datetime(year, month, 1).date()

            db.save_transaction(user_id, {
                "date":       date_obj.strftime("%d.%m.%Y"),
                "section":    sec,
                "category":   cat,
                "amount":     exp_amt,
                "currency":   "RUB",
                "rate":       1.0,
                "amount_rub": exp_amt,
                "tx_type":    "Расход",
                "merchant":   "",
                "comment":    f"Импорт из Sheets {year}",
                "source":     "sheets_import",
            })
            count += 1

    return count


def import_from_sheets(user_id: int, year: int, months: list = None):
    """Основная функция импорта."""
    log.info(f"Подключаюсь к Google Sheets...")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)
    gc    = gspread.authorize(creds)
    sh    = gc.open_by_url(SHEET_URL)

    worksheets     = {ws.title: ws for ws in sh.worksheets()}
    total_imported = 0

    for sheet_name, month_num in MONTH_SHEET_NAMES.items():
        if months and month_num not in months:
            continue

        if sheet_name not in worksheets:
            log.warning(f"Лист '{sheet_name}' не найден — пропускаем")
            continue

        log.info(f"Импортирую {sheet_name} {year}...")
        try:
            ws    = worksheets[sheet_name]
            count = import_month_sheet(ws, year, month_num, user_id)
            # Записываем период с уровнем полноты
            db.upsert_period(user_id, year, month_num, completeness="full")
            log.info(f"  ✅ {sheet_name}: {count} транзакций")
            total_imported += count
        except Exception as e:
            log.error(f"  ❌ {sheet_name}: {e}")

    log.info(f"\n🏁 Импорт завершён. Всего: {total_imported} транзакций за {year} год.")
    return total_imported


def import_summary_only(user_id: int, year: int, month: int,
                         income: float, expense: float, notes: str = ""):
    """
    Импорт периода без детализации (summary режим).
    Используется для исторических данных где нет категорий.
    """
    db.upsert_period(
        user_id, year, month,
        completeness="summary",
        total_income=income,
        total_expense=expense,
        notes=notes
    )
    log.info(f"✅ Summary период: {MONTH_NAMES.get(month,'')} {year} — доход {income}, расход {expense}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Импорт истории из Google Sheets в PostgreSQL")
    parser.add_argument("--user_id", type=int, required=True, help="Telegram user ID")
    parser.add_argument("--year",    type=int, required=True, help="Год для импорта")
    parser.add_argument("--months",  type=str, default="",   help="Месяцы через запятую (1,2,3). По умолчанию все.")
    args = parser.parse_args()

    months_list = [int(m.strip()) for m in args.months.split(",") if m.strip()] or None

    db.init_db()
    import_from_sheets(args.user_id, args.year, months_list)
