# -*- coding: utf-8 -*-
"""
DizelFinance — PostgreSQL через psycopg2 (sync, для совместимости с Flask + aiogram2)
"""

import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from datetime import datetime
from config import DATABASE_URL

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Соединение
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@contextmanager
def db():
    conn = get_conn()
    try:
        with conn:
            yield conn
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# Инициализация схемы
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    date          DATE         NOT NULL,
    section       TEXT         NOT NULL,   -- Доходы / Регулярные расходы / Крупные траты / Движение активов
    category      TEXT         NOT NULL,
    amount        NUMERIC(14,2) NOT NULL,
    currency      TEXT         NOT NULL DEFAULT 'RUB',
    rate          NUMERIC(14,6) NOT NULL DEFAULT 1,
    amount_rub    NUMERIC(14,2) NOT NULL,
    tx_type       TEXT         NOT NULL DEFAULT 'Расход',  -- Доход / Расход / Актив
    merchant      TEXT,
    comment       TEXT,
    source        TEXT         DEFAULT 'manual',  -- manual / sms / screenshot / pdf / xlsx
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS periods (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    year          INT          NOT NULL,
    month         INT,                         -- NULL = годовой период
    completeness  TEXT         NOT NULL DEFAULT 'full',  -- full / summary / minimal / capital_only
    total_income  NUMERIC(14,2),
    total_expense NUMERIC(14,2),
    total_assets  NUMERIC(14,2),
    notes         TEXT,
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(user_id, year, month)
);

CREATE TABLE IF NOT EXISTS category_aliases (
    id            SERIAL PRIMARY KEY,
    old_name      TEXT NOT NULL,
    new_name      TEXT NOT NULL,
    valid_from    DATE,
    valid_to      DATE
);

CREATE TABLE IF NOT EXISTS drafts (
    id            TEXT PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    amount        NUMERIC(14,2),
    currency      TEXT,
    rate          NUMERIC(14,6),
    amount_rub    NUMERIC(14,2),
    date          TEXT,
    merchant      TEXT,
    tx_type       TEXT DEFAULT 'Расход',
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_user_date  ON transactions(user_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_category   ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_tx_section    ON transactions(section);
CREATE INDEX IF NOT EXISTS idx_periods_user  ON periods(user_id, year, month);
"""

def init_db():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    log.info("✅ БД инициализирована")

# ══════════════════════════════════════════════════════════════════════════════
# Транзакции
# ══════════════════════════════════════════════════════════════════════════════

def save_transaction(user_id: int, data: dict) -> int:
    sql = """
        INSERT INTO transactions
            (user_id, date, section, category, amount, currency, rate,
             amount_rub, tx_type, merchant, comment, source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """
    date = _parse_date(data.get("date", ""))
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                user_id,
                date,
                data.get("section", ""),
                data.get("category", ""),
                data.get("amount", 0),
                data.get("currency", "RUB"),
                data.get("rate", 1.0),
                data.get("amount_rub", data.get("amount", 0)),
                data.get("tx_type", "Расход"),
                data.get("merchant", ""),
                data.get("comment", ""),
                data.get("source", "manual"),
            ))
            row_id = cur.fetchone()[0]
    return row_id

def save_transactions_batch(user_id: int, items: list[dict]) -> int:
    if not items:
        return 0
    sql = """
        INSERT INTO transactions
            (user_id, date, section, category, amount, currency, rate,
             amount_rub, tx_type, merchant, comment, source)
        VALUES %s
    """
    rows = []
    for data in items:
        date = _parse_date(data.get("date", ""))
        rows.append((
            user_id,
            date,
            data.get("section", ""),
            data.get("category", ""),
            data.get("amount", 0),
            data.get("currency", "RUB"),
            data.get("rate", 1.0),
            data.get("amount_rub", data.get("amount", 0)),
            data.get("tx_type", "Расход"),
            data.get("merchant", ""),
            data.get("comment", ""),
            data.get("source", "manual"),
        ))
    with db() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows)
    return len(rows)

def get_transactions(user_id: int, limit: int = 50, offset: int = 0,
                     year: int = None, month: int = None,
                     section: str = None, category: str = None) -> list[dict]:
    conditions = ["user_id = %s"]
    params: list = [user_id]

    if year:
        conditions.append("EXTRACT(YEAR FROM date) = %s")
        params.append(year)
    if month:
        conditions.append("EXTRACT(MONTH FROM date) = %s")
        params.append(month)
    if section:
        conditions.append("section = %s")
        params.append(section)
    if category:
        conditions.append("category = %s")
        params.append(category)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, date, section, category, amount, currency, rate,
               amount_rub, tx_type, merchant, comment, source, created_at
        FROM transactions
        WHERE {where}
        ORDER BY date DESC, created_at DESC
        LIMIT %s OFFSET %s
    """
    params += [limit, offset]
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

def count_transactions(user_id: int, year: int = None, month: int = None) -> int:
    conditions = ["user_id = %s"]
    params: list = [user_id]
    if year:
        conditions.append("EXTRACT(YEAR FROM date) = %s")
        params.append(year)
    if month:
        conditions.append("EXTRACT(MONTH FROM date) = %s")
        params.append(month)
    sql = f"SELECT COUNT(*) FROM transactions WHERE {' AND '.join(conditions)}"
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

def delete_transaction(tx_id: int, user_id: int) -> bool:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE id=%s AND user_id=%s", (tx_id, user_id))
            return cur.rowcount > 0

def get_existing_keys(user_id: int) -> set:
    """Для дедупликации при импорте: date|amount_rub"""
    sql = """
        SELECT date::text, ROUND(amount_rub,2)::text
        FROM transactions WHERE user_id=%s
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            return {f"{r[0]}|{r[1]}" for r in cur.fetchall()}

# ══════════════════════════════════════════════════════════════════════════════
# Аналитика
# ══════════════════════════════════════════════════════════════════════════════

def get_monthly_summary(user_id: int, year: int, month: int) -> dict:
    sql = """
        SELECT
            section,
            SUM(amount_rub) FILTER (WHERE tx_type = 'Доход') AS income,
            SUM(amount_rub) FILTER (WHERE tx_type = 'Расход') AS expense,
            SUM(amount_rub) FILTER (WHERE tx_type = 'Актив')  AS assets,
            COUNT(*) AS cnt
        FROM transactions
        WHERE user_id=%s
          AND EXTRACT(YEAR FROM date) = %s
          AND EXTRACT(MONTH FROM date) = %s
        GROUP BY section
        ORDER BY section
    """
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (user_id, year, month))
            rows = [dict(r) for r in cur.fetchall()]

    total_income  = sum(float(r["income"]  or 0) for r in rows)
    total_expense = sum(float(r["expense"] or 0) for r in rows)
    total_assets  = sum(float(r["assets"]  or 0) for r in rows)
    return {
        "sections": rows,
        "total_income":  total_income,
        "total_expense": total_expense,
        "total_assets":  total_assets,
        "delta":         total_income - total_expense,
    }

def get_category_breakdown(user_id: int, year: int, month: int = None,
                            section: str = None) -> list[dict]:
    conditions = ["user_id=%s", "EXTRACT(YEAR FROM date)=%s"]
    params: list = [user_id, year]
    if month:
        conditions.append("EXTRACT(MONTH FROM date)=%s")
        params.append(month)
    if section:
        conditions.append("section=%s")
        params.append(section)
    sql = f"""
        SELECT category, section, SUM(amount_rub) AS total, COUNT(*) AS cnt
        FROM transactions
        WHERE {' AND '.join(conditions)}
        GROUP BY category, section
        ORDER BY total DESC
    """
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

def get_yearly_trend(user_id: int, years: int = 3) -> list[dict]:
    current_year = datetime.now().year
    sql = """
        SELECT
            EXTRACT(YEAR FROM date)::int  AS year,
            EXTRACT(MONTH FROM date)::int AS month,
            SUM(amount_rub) FILTER (WHERE tx_type='Доход')  AS income,
            SUM(amount_rub) FILTER (WHERE tx_type='Расход') AS expense
        FROM transactions
        WHERE user_id=%s AND EXTRACT(YEAR FROM date) >= %s
        GROUP BY year, month
        ORDER BY year, month
    """
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (user_id, current_year - years + 1))
            return [dict(r) for r in cur.fetchall()]

def get_top_categories(user_id: int, year: int, month: int = None,
                       limit: int = 10) -> list[dict]:
    conditions = ["user_id=%s", "EXTRACT(YEAR FROM date)=%s", "tx_type='Расход'"]
    params: list = [user_id, year]
    if month:
        conditions.append("EXTRACT(MONTH FROM date)=%s")
        params.append(month)
    sql = f"""
        SELECT category, SUM(amount_rub) AS total
        FROM transactions
        WHERE {' AND '.join(conditions)}
        GROUP BY category
        ORDER BY total DESC
        LIMIT %s
    """
    params.append(limit)
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

# ══════════════════════════════════════════════════════════════════════════════
# Периоды (уровни полноты)
# ══════════════════════════════════════════════════════════════════════════════

def upsert_period(user_id: int, year: int, month: int = None,
                  completeness: str = "full", **kwargs):
    sql = """
        INSERT INTO periods (user_id, year, month, completeness,
                             total_income, total_expense, total_assets, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id, year, month) DO UPDATE SET
            completeness  = EXCLUDED.completeness,
            total_income  = COALESCE(EXCLUDED.total_income,  periods.total_income),
            total_expense = COALESCE(EXCLUDED.total_expense, periods.total_expense),
            total_assets  = COALESCE(EXCLUDED.total_assets,  periods.total_assets),
            notes         = COALESCE(EXCLUDED.notes,         periods.notes)
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                user_id, year, month, completeness,
                kwargs.get("total_income"),
                kwargs.get("total_expense"),
                kwargs.get("total_assets"),
                kwargs.get("notes"),
            ))

def get_periods(user_id: int) -> list[dict]:
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM periods WHERE user_id=%s ORDER BY year DESC, month DESC",
                (user_id,)
            )
            return [dict(r) for r in cur.fetchall()]

# ══════════════════════════════════════════════════════════════════════════════
# Черновики
# ══════════════════════════════════════════════════════════════════════════════

def drafts_add(user_id: int, draft: dict):
    sql = """
        INSERT INTO drafts (id, user_id, amount, currency, rate, amount_rub,
                            date, merchant, tx_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                draft["id"], user_id, draft["a"], draft["cur"],
                draft["rate"], draft["a_rub"], draft["d"],
                draft["m"], draft.get("tx_type", "Расход")
            ))

def drafts_get(user_id: int) -> list[dict]:
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id,amount,currency,rate,amount_rub,date,merchant,tx_type "
                "FROM drafts WHERE user_id=%s ORDER BY created_at DESC",
                (user_id,)
            )
            return [
                {"id": r["id"], "a": float(r["amount"]), "cur": r["currency"],
                 "rate": float(r["rate"]), "a_rub": float(r["amount_rub"]),
                 "d": r["date"], "m": r["merchant"], "tx_type": r["tx_type"]}
                for r in cur.fetchall()
            ]

def drafts_remove(draft_id: str):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM drafts WHERE id=%s", (draft_id,))

def drafts_clear(user_id: int):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM drafts WHERE user_id=%s", (user_id,))

# ══════════════════════════════════════════════════════════════════════════════
# Утилиты
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date(date_str: str):
    """Конвертирует строку ДД.ММ.ГГГГ в объект date."""
    for fmt in ("%d.%m.%Y, %H:%M", "%d.%m.%Y,%H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return datetime.now().date()
