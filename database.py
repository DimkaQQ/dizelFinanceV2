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

# ═══════════════════════════════════════════════════════════════════════════════
# Соединение
# ═══════════════════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════════════════
# Инициализация схемы
# ═══════════════════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    date          DATE         NOT NULL,
    section       TEXT         NOT NULL,
    category      TEXT         NOT NULL,
    amount        NUMERIC(14,2) NOT NULL,
    currency      TEXT         NOT NULL DEFAULT 'RUB',
    rate          NUMERIC(14,6) NOT NULL DEFAULT 1,
    amount_rub    NUMERIC(14,2) NOT NULL,
    tx_type       TEXT         NOT NULL DEFAULT 'Расход',
    merchant      TEXT,
    comment       TEXT,
    source        TEXT         DEFAULT 'manual',
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS periods (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    year          INT          NOT NULL,
    month         INT,
    completeness  TEXT         NOT NULL DEFAULT 'full',
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

-- 🔥 НОВАЯ ТАБЛИЦА: Корректировки итоговых значений
CREATE TABLE IF NOT EXISTS adjusted_totals (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    year          INT NOT NULL,
    month         INT,
    metric_type   TEXT NOT NULL,
    adjusted_value NUMERIC(14,2) NOT NULL,
    note          TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, year, month, metric_type)
);

-- 🔥 НОВАЯ ТАБЛИЦА: Пользовательские категории
CREATE TABLE IF NOT EXISTS custom_categories (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    name          TEXT NOT NULL,
    section       TEXT NOT NULL,
    is_active     BOOLEAN DEFAULT true,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, name)
);

-- 🔥 НОВАЯ ТАБЛИЦА: Корректировки категорий
CREATE TABLE IF NOT EXISTS category_adjustments (
    id            SERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    year          INT NOT NULL,
    month         INT,
    category      TEXT NOT NULL,
    adjusted_value NUMERIC(14,2) NOT NULL,
    note          TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, year, month, category)
);

CREATE INDEX IF NOT EXISTS idx_cat_adj_user ON category_adjustments(user_id, year, month);

CREATE INDEX IF NOT EXISTS idx_tx_user_date  ON transactions(user_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_category   ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_tx_section    ON transactions(section);
CREATE INDEX IF NOT EXISTS idx_periods_user  ON periods(user_id, year, month);
CREATE INDEX IF NOT EXISTS idx_adjusted_user ON adjusted_totals(user_id, year, month);
CREATE INDEX IF NOT EXISTS idx_custom_cat_user ON custom_categories(user_id, is_active);
"""

def init_db():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        log.info("✅ БД инициализирована")

# ═══════════════════════════════════════════════════════════════════════════════
# Транзакции
# ═══════════════════════════════════════════════════════════════════════════════

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
    sql = """
    SELECT date::text, ROUND(amount_rub,2)::text
    FROM transactions WHERE user_id=%s
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            return {f"{r[0]}|{r[1]}" for r in cur.fetchall()}

# ═══════════════════════════════════════════════════════════════════════════════
# 🔥 КОРРЕКТИРОВКИ ИТОГОВЫХ ЗНАЧЕНИЙ
# ═══════════════════════════════════════════════════════════════════════════════

def save_adjusted_total(user_id: int, year: int, month: int, 
                        metric_type: str, value: float, note: str = ""):
    """Сохраняет корректировку для дохода/расхода/активов."""
    sql = """
    INSERT INTO adjusted_totals (user_id, year, month, metric_type, adjusted_value, note, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s, NOW())
    ON CONFLICT (user_id, year, month, metric_type) 
    DO UPDATE SET adjusted_value = EXCLUDED.adjusted_value, 
                  note = EXCLUDED.note,
                  updated_at = NOW()
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, year, month, metric_type, value, note))

def get_adjusted_total(user_id: int, year: int, month: int, metric_type: str):
    """Получает корректировку если есть."""
    sql = """
    SELECT adjusted_value, note FROM adjusted_totals
    WHERE user_id=%s AND year=%s AND month=%s AND metric_type=%s
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, year, month, metric_type))
            row = cur.fetchone()
            return (float(row[0]), row[1]) if row else (None, None)

def get_adjusted_totals_for_period(user_id: int, year: int, month: int = None):
    """Получает все корректировки за период."""
    if month is None:
        sql = """
        SELECT metric_type, adjusted_value, note 
        FROM adjusted_totals
        WHERE user_id=%s AND year=%s AND month IS NULL
        """
        params = (user_id, year)
    else:
        sql = """
        SELECT metric_type, adjusted_value, note 
        FROM adjusted_totals
        WHERE user_id=%s AND year=%s AND month=%s
        """
        params = (user_id, year, month)
    
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return {row[0]: {"value": float(row[1]), "note": row[2]} for row in cur.fetchall()}

# ═══════════════════════════════════════════════════════════════════════════════
# 🔥 УПРАВЛЕНИЕ КАТЕГОРИЯМИ
# ═══════════════════════════════════════════════════════════════════════════════

def add_custom_category(user_id: int, name: str, section: str):
    """Добавляет пользовательскую категорию."""
    sql = """
    INSERT INTO custom_categories (user_id, name, section, is_active, updated_at)
    VALUES (%s,%s,%s,true, NOW())
    ON CONFLICT (user_id, name) DO UPDATE SET section=EXCLUDED.section, is_active=true, updated_at=NOW()
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, name, section))

def remove_custom_category(user_id: int, name: str):
    """Деактивирует пользовательскую категорию."""
    sql = """
    UPDATE custom_categories 
    SET is_active=false, updated_at=NOW()
    WHERE user_id=%s AND name=%s
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, name))
            return cur.rowcount > 0

def get_custom_categories(user_id: int, section: str = None):
    """Получает активные пользовательские категории."""
    if section:
        sql = """
        SELECT name, section FROM custom_categories 
        WHERE user_id=%s AND section=%s AND is_active=true
        ORDER BY name
        """
        params = (user_id, section)
    else:
        sql = """
        SELECT name, section FROM custom_categories 
        WHERE user_id=%s AND is_active=true
        ORDER BY section, name
        """
        params = (user_id,)
    
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [{"name": row[0], "section": row[1]} for row in cur.fetchall()]

def get_categories_for_section(user_id: int, section: str):
    """Получает все категории для секции (кастомные + дефолтные)."""
    from config import SECTIONS
    
    # Дефолтные категории из config
    default_cats = SECTIONS.get(section, {}).get("categories", [])
    
    # Пользовательские категории
    custom = get_custom_categories(user_id, section)
    custom_names = [c["name"] for c in custom]
    
    # Объединяем без дублей, приоритет у кастомных
    all_cats = custom_names + [c for c in default_cats if c not in custom_names]
    return all_cats

def migrate_portfolio_names(user_id: int):
    """Заменяет старые названия портфелей на новые."""
    sql = """
    UPDATE transactions 
    SET category = 'Портфель детей', 
        section = 'Движение активов'
    WHERE user_id = %s 
      AND category IN ('Портфель Екатерины', 'Портфель Влада')
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            return cur.rowcount

# ═══════════════════════════════════════════════════════════════════════════════
# Аналитика
# ═══════════════════════════════════════════════════════════════════════════════

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
            
            # 🔥 Конвертируем Decimal в float
            for row in rows:
                row['income'] = float(row['income']) if row['income'] else 0.0
                row['expense'] = float(row['expense']) if row['expense'] else 0.0
                row['assets'] = float(row['assets']) if row['assets'] else 0.0
                row['cnt'] = int(row['cnt']) if row['cnt'] else 0
            
            total_income  = sum(row['income'] for row in rows)
            total_expense = sum(row['expense'] for row in rows)
            total_assets  = sum(row['assets'] for row in rows)
            
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
            rows = [dict(r) for r in cur.fetchall()]
            
            # 🔥 Конвертируем Decimal в float для Jinja2
            for row in rows:
                row['total'] = float(row['total']) if row['total'] else 0.0
                row['cnt'] = int(row['cnt']) if row['cnt'] else 0
            
            # 🔥 ПРИМЕНЯЕМ КОРРЕКТИРОВКИ (ЭТОГО НЕ ХВАТАЛО!)
            adjustments = get_category_adjustments_for_period(user_id, year, month)
            for row in rows:
                if row['category'] in adjustments:
                    row['total'] = adjustments[row['category']]['value']
            
            return rows

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

# ═══════════════════════════════════════════════════════════════════════════════
# 🔥 КОРРЕКТИРОВКИ КАТЕГОРИЙ
# ═══════════════════════════════════════════════════════════════════════════════

def save_category_adjustment(user_id: int, year: int, month: int, 
                             category: str, adjusted_value: float, note: str = ""):
    """Сохраняет корректировку для конкретной категории."""
    sql = """
    INSERT INTO category_adjustments (user_id, year, month, category, adjusted_value, note, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s, NOW())
    ON CONFLICT (user_id, year, month, category) 
    DO UPDATE SET adjusted_value = EXCLUDED.adjusted_value, 
                  note = EXCLUDED.note,
                  updated_at = NOW()
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, year, month, category, adjusted_value, note))

def get_category_adjustment(user_id: int, year: int, month: int, category: str):
    """Получает корректировку для категории если есть."""
    sql = """
    SELECT adjusted_value, note FROM category_adjustments
    WHERE user_id=%s AND year=%s AND month=%s AND category=%s
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, year, month, category))
            row = cur.fetchone()
            return (float(row[0]), row[1]) if row else (None, None)

def get_category_adjustments_for_period(user_id: int, year: int, month: int = None):
    """Получает все корректировки категорий за период."""
    if month is None:
        sql = """
        SELECT category, adjusted_value, note 
        FROM category_adjustments
        WHERE user_id=%s AND year=%s AND month IS NULL
        """
        params = (user_id, year)
    else:
        sql = """
        SELECT category, adjusted_value, note 
        FROM category_adjustments
        WHERE user_id=%s AND year=%s AND month=%s
        """
        params = (user_id, year, month)
    
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return {row[0]: {"value": float(row[1]), "note": row[2]} for row in cur.fetchall()}

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

# ═══════════════════════════════════════════════════════════════════════════════
# Периоды (уровни полноты)
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_period(user_id: int, year: int, month: int = None,
                  completeness: str = "full", **kwargs):
    sql = """
    INSERT INTO periods (user_id, year, month, completeness,
                         total_income, total_expense, total_assets, notes)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (user_id, year, month) DO  UPDATE SET
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

# ═══════════════════════════════════════════════════════════════════════════════
# Черновики
# ═══════════════════════════════════════════════════════════════════════════════

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
                "FROM drafts WHERE user_id=%s ORDER BY created_at DESC ",
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

# ═══════════════════════════════════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_date(date_str: str):
    """Конвертирует строку ДД.ММ.ГГГГ в объект date."""
    for fmt in ("%d.%m.%Y, %H:%M", "%d.%m.%Y,%H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return datetime.now().date()

def get_monthly_summary_year(user_id: int, year: int) -> list[dict]:
    """Сводка по каждому месяцу года."""
    sql = """
    SELECT
        EXTRACT(MONTH FROM date)::int AS month,
        SUM(amount_rub) FILTER (WHERE tx_type='Доход')  AS income,
        SUM(amount_rub) FILTER (WHERE tx_type='Расход') AS expense,
        SUM(amount_rub) FILTER (WHERE tx_type='Актив')  AS assets,
        COUNT(*) AS cnt
    FROM transactions
    WHERE user_id=%s AND EXTRACT(YEAR FROM date)=%s
    GROUP BY month
    ORDER BY month
    """
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (user_id, year))
            return [dict(r) for r in cur.fetchall()]

def get_month_expenses_list(user_id: int, year: int, month: int,
                            limit: int = 10, offset: int = 0) -> list[dict]:
    """Все расходы за месяц постранично."""
    sql = """
    SELECT id, date, category, section, amount, currency, amount_rub,
           merchant, tx_type
    FROM transactions
    WHERE user_id=%s
    AND EXTRACT(YEAR FROM date)=%s
    AND EXTRACT(MONTH FROM date)=%s
    AND tx_type='Расход'
    ORDER BY date DESC, created_at DESC
    LIMIT %s OFFSET %s
    """
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (user_id, year, month, limit, offset))
            return [dict(r) for r in cur.fetchall()]

def count_month_expenses(user_id: int, year: int, month: int) -> int:
    sql = """
    SELECT COUNT(*) FROM transactions
    WHERE user_id=%s
    AND EXTRACT(YEAR FROM date)=%s
    AND EXTRACT(MONTH FROM date)=%s
    AND tx_type='Расход'
    """
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, year, month))
            return cur.fetchone()[0]

def get_compare_months(user_id: int,
                       year1: int, month1: int,
                       year2: int, month2: int) -> dict:
    """Сравнение двух месяцев по разделам."""
    def _fetch(y, m):
        sql = """
        SELECT section,
               SUM(amount_rub) FILTER (WHERE tx_type='Доход')  AS income,
               SUM(amount_rub) FILTER (WHERE tx_type='Расход') AS expense,
               COUNT(*) AS cnt
        FROM transactions
        WHERE user_id=%s
        AND EXTRACT(YEAR FROM date)=%s
        AND EXTRACT(MONTH FROM date)=%s
        GROUP BY section
        """
        with db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (user_id, y, m))
                return {r["section"]: dict(r) for r in cur.fetchall()}
    
    d1 = _fetch(year1, month1)
    d2 = _fetch(year2, month2)
    all_sections = set(d1) | set(d2)
    result = []
    for sec in all_sections:
        r1 = d1.get(sec, {})
        r2 = d2.get(sec, {})
        exp1 = float(r1.get("expense") or 0)
        exp2 = float(r2.get("expense") or 0)
        inc1 = float(r1.get("income")  or 0)
        inc2 = float(r2.get("income")  or 0)
        result.append({
            "section": sec,
            "expense1": exp1, "expense2": exp2,
            "income1":  inc1, "income2":  inc2,
            "exp_diff": exp2 - exp1,
            "exp_pct":  round((exp2 - exp1) / exp1 * 100, 1) if exp1 else 0,
        })
    return {
        "sections": sorted(result, key=lambda x: x["expense1"], reverse=True),
        "total1": {
            "income":  sum(float(v.get("income")  or 0) for v in d1.values()),
            "expense": sum(float(v.get("expense") or 0) for v in d1.values()),
        },
        "total2": {
            "income":  sum(float(v.get("income")  or 0) for v in d2.values()),
            "expense": sum(float(v.get("expense") or 0) for v in d2.values()),
        },
    }