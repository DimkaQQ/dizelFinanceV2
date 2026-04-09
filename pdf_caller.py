# -*- coding: utf-8 -*-
"""
DizelFinance — клиент для PDF сервиса (Puppeteer)
Вызывается из bot.py и web/app.py
"""

import os
import logging
import requests
from datetime import datetime

log = logging.getLogger(__name__)

PDF_SERVICE_URL = os.getenv("PDF_SERVICE_URL", "http://localhost:3001")


# ══════════════════════════════════════════════════════════════════════════════
# Утилиты форматирования
# ══════════════════════════════════════════════════════════════════════════════

def _fmt(val: float) -> str:
    """Форматирует число с пробелами как разделитель тысяч."""
    if val is None:
        return "0"
    return f"{abs(val):,.0f}".replace(",", " ")


def _fmt_signed(val: float) -> str:
    """Форматирует число со знаком +/–."""
    sign = "+" if val >= 0 else "–"
    return f"{sign} {_fmt(abs(val))}"


def _pct(part: float, total: float) -> float:
    """Считает процент, избегая деления на ноль."""
    if not total:
        return 0.0
    return round(part / total * 100, 1)


def _convert_date(date_val):
    """Конвертирует date/datetime в строку ДД.ММ.ГГГГ."""
    if date_val is None:
        return ""
    if hasattr(date_val, "strftime"):
        return date_val.strftime("%d.%m.%Y")
    return str(date_val)


# ══════════════════════════════════════════════════════════════════════════════
# Основной клиент PDF сервиса
# ══════════════════════════════════════════════════════════════════════════════

def generate_pdf(template: str, data: dict) -> bytes:
    """
    Вызывает Node.js PDF сервис.
    
    Args:
        template: monthly | quarterly | yearly | networth | comparative
        data: словарь с данными для шаблона
    
    Returns:
        bytes: PDF файл
    
    Raises:
        RuntimeError: если сервис недоступен или вернул ошибку
    """
    try:
        resp = requests.post(
            f"{PDF_SERVICE_URL}/generate",
            json={"template": template, "data": data},
            timeout=60,  # Увеличили таймаут для сложных отчётов
        )
        if resp.status_code == 200:
            return resp.content
        else:
            log.error(f"PDF service error {resp.status_code}: {resp.text[:300]}")
            raise RuntimeError(f"PDF сервис вернул {resp.status_code}")
    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            "PDF сервис недоступен. Запусти: cd pdf_service && node server.js"
        )
    except requests.exceptions.Timeout:
        raise RuntimeError("Таймаут при генерации PDF. Попробуйте позже.")


# ══════════════════════════════════════════════════════════════════════════════
# Сбор данных для шаблонов
# ══════════════════════════════════════════════════════════════════════════════

def build_monthly_data(uid: int, year: int, month: int) -> dict:
    """
    Собирает все данные для monthly.html шаблона.
    
    Ключи, ожидаемые шаблоном:
    - period, month_name, year, generated_at
    - total_income, total_expense, total_assets, delta (+ _fmt версии)
    - sections: [{name, expense, expense_fmt, pct}]
    - top_categories: [{name, total, total_fmt}]
    - transactions: [{date, category, merchant, tx_type, amount, currency, amount_fmt}]
    - trend: [{label, income, expense}]
    - prev_income, prev_expense, prev_delta (для сравнения с прошлым месяцем)
    """
    import database as db
    from config import MONTH_NAMES
    
    summary = db.get_monthly_summary(uid, year, month)
    top_cats = db.get_top_categories(uid, year, month, limit=15)
    txs = db.get_month_expenses_list(uid, year, month, limit=30, offset=0)
    trend = db.get_yearly_trend(uid, years=2)
    
    # Данные прошлого месяца для сравнения
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    prev = db.get_monthly_summary(uid, prev_y, prev_m)
    
    # Конвертируем даты в строки
    for tx in txs:
        if hasattr(tx.get("date"), "strftime"):
            tx["date"] = tx["date"].strftime("%d.%m.%Y")
        tx["amount_fmt"] = _fmt(float(tx.get("amount") or 0))
    
    month_name = MONTH_NAMES.get(month, "")
    total_expense = float(summary["total_expense"] or 0)
    
    return {
        # Период
        "year": year,
        "month": month,
        "month_name": month_name,
        "period": f"{month_name} {year}",
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        
        # Основные показатели
        "total_income": float(summary["total_income"] or 0),
        "total_income_fmt": _fmt(float(summary["total_income"] or 0)),
        "total_expense": total_expense,
        "total_expense_fmt": _fmt(total_expense),
        "total_assets": float(summary["total_assets"] or 0),
        "total_assets_fmt": _fmt(float(summary["total_assets"] or 0)),
        "delta": float(summary["delta"] or 0),
        "delta_fmt": _fmt_signed(float(summary["delta"] or 0)),
        "delta_sign": "📈" if (summary["delta"] or 0) >= 0 else "⚠️",
        "delta_label": "Профицит" if (summary["delta"] or 0) >= 0 else "Дефицит",
        
        # Прошлый месяц для сравнения
        "prev_period": f"{MONTH_NAMES.get(prev_m, '')} {prev_y}",
        "prev_income": float(prev["total_income"] or 0),
        "prev_income_fmt": _fmt(float(prev["total_income"] or 0)),
        "prev_expense": float(prev["total_expense"] or 0),
        "prev_expense_fmt": _fmt(float(prev["total_expense"] or 0)),
        "prev_delta": float(prev["delta"] or 0),
        "prev_delta_fmt": _fmt_signed(float(prev["delta"] or 0)),
        
        # Структура расходов по разделам
        "sections": [
            {
                "name": s["section"],
                "expense": float(s["expense"] or 0),
                "expense_fmt": _fmt(float(s["expense"] or 0)),
                "pct": _pct(float(s["expense"] or 0), total_expense),
            }
            for s in summary.get("sections", [])
            if float(s.get("expense") or 0) > 0
        ],
        
        # Топ категорий расходов
        "top_categories": [
            {
                "name": c["category"],
                "total": float(c["total"] or 0),
                "total_fmt": _fmt(float(c["total"] or 0)),
                "pct": _pct(float(c["total"] or 0), total_expense),
            }
            for c in top_cats
        ],
        
        # Транзакции (превью)
        "transactions": [
            {
                "date": tx["date"],
                "category": tx.get("category", ""),
                "merchant": tx.get("merchant", ""),
                "tx_type": tx.get("tx_type", ""),
                "amount": float(tx.get("amount") or 0),
                "currency": tx.get("currency", "RUB"),
                "amount_fmt": tx.get("amount_fmt", _fmt(float(tx.get("amount") or 0))),
                "amount_rub": float(tx.get("amount_rub") or tx.get("amount") or 0),
                "amount_rub_fmt": _fmt(float(tx.get("amount_rub") or tx.get("amount") or 0)),
            }
            for tx in txs
        ],
        
        # Тренд для графика (доходы/расходы по месяцам)
        "trend": [
            {
                "label": f"{MONTH_NAMES.get(r['month'],'')[:3]} {r['year']}",
                "income": float(r.get("income") or 0),
                "expense": float(r.get("expense") or 0),
                "income_fmt": _fmt(float(r.get("income") or 0)),
                "expense_fmt": _fmt(float(r.get("expense") or 0)),
            }
            for r in trend
        ],
        
        # Норма сбережений
        "savings_rate": _pct(
            max(0, float(summary["total_income"] or 0) - float(summary["total_expense"] or 0)),
            float(summary["total_income"] or 1)
        ),
    }


def build_quarterly_data(uid: int, year: int, quarter: int) -> dict:
    """
    Собирает данные для quarterly.html шаблона.
    
    Args:
        quarter: 1, 2, 3 или 4
    """
    import database as db
    from config import MONTH_NAMES
    
    months = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
    q_months = months.get(quarter, [1, 2, 3])
    
    total_income = total_expense = total_assets = 0.0
    monthly_data = []
    all_txs = []
    
    for m in q_months:
        s = db.get_monthly_summary(uid, year, m)
        inc = float(s["total_income"] or 0)
        exp = float(s["total_expense"] or 0)
        ast = float(s["total_assets"] or 0)
        
        total_income += inc
        total_expense += exp
        total_assets += ast
        
        monthly_data.append({
            "month": m,
            "month_name": MONTH_NAMES.get(m, ""),
            "income": inc,
            "income_fmt": _fmt(inc),
            "expense": exp,
            "expense_fmt": _fmt(exp),
            "delta": inc - exp,
            "delta_fmt": _fmt_signed(inc - exp),
        })
        
        txs = db.get_month_expenses_list(uid, year, m, limit=100, offset=0)
        for tx in txs:
            if hasattr(tx.get("date"), "strftime"):
                tx["date"] = tx["date"].strftime("%d.%m.%Y")
            tx["amount_fmt"] = _fmt(float(tx.get("amount") or 0))
        all_txs.extend(txs)
    
    # Топ категорий за квартал
    top_cats = db.get_category_breakdown(uid, year=year, section="Регулярные расходы")[:15]
    
    delta = total_income - total_expense
    savings_rate = _pct(max(0, delta), total_income) if total_income else 0
    
    return {
        # Период
        "year": year,
        "quarter": quarter,
        "quarter_label": f"Q{quarter} {year}",
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        
        # Итоги за квартал
        "total_income": total_income,
        "total_income_fmt": _fmt(total_income),
        "total_expense": total_expense,
        "total_expense_fmt": _fmt(total_expense),
        "total_assets": total_assets,
        "total_assets_fmt": _fmt(total_assets),
        "delta": delta,
        "delta_fmt": _fmt_signed(delta),
        "delta_sign": "📈" if delta >= 0 else "⚠️",
        "savings_rate": savings_rate,
        
        # Помесячная разбивка
        "monthly_data": monthly_data,
        
        # Топ категорий
        "top_categories": [
            {
                "name": c["category"],
                "total": float(c["total"] or 0),
                "total_fmt": _fmt(float(c["total"] or 0)),
            }
            for c in top_cats
        ],
        
        # Транзакции (превью)
        "transactions": [
            {
                "date": tx["date"],
                "category": tx.get("category", ""),
                "merchant": tx.get("merchant", ""),
                "tx_type": tx.get("tx_type", ""),
                "amount": float(tx.get("amount") or 0),
                "currency": tx.get("currency", "RUB"),
                "amount_fmt": tx.get("amount_fmt", ""),
            }
            for tx in all_txs[:40]  # лимит для отчёта
        ],
    }


def build_yearly_data(uid: int, year: int) -> dict:
    """Собирает данные для yearly.html шаблона."""
    import database as db
    from config import MONTH_NAMES
    
    monthly = db.get_monthly_summary_year(uid, year)
    top_cats = db.get_top_categories(uid, year, limit=20)
    trend = db.get_yearly_trend(uid, years=3)
    
    total_income = sum(float(r.get("income") or 0) for r in monthly)
    total_expense = sum(float(r.get("expense") or 0) for r in monthly)
    total_assets = sum(float(r.get("assets") or 0) for r in monthly)
    delta = total_income - total_expense
    savings_rate = _pct(max(0, delta), total_income) if total_income else 0
    
    # Квартальная агрегация
    quarters = []
    for q_num, q_months in [(1, [1,2,3]), (2, [4,5,6]), (3, [7,8,9]), (4, [10,11,12])]:
        q_inc = sum(float(r.get("income") or 0) for r in monthly if r["month"] in q_months)
        q_exp = sum(float(r.get("expense") or 0) for r in monthly if r["month"] in q_months)
        quarters.append({
            "label": f"Q{q_num}",
            "income": q_inc,
            "income_fmt": _fmt(q_inc),
            "expense": q_exp,
            "expense_fmt": _fmt(q_exp),
            "delta": q_inc - q_exp,
            "delta_fmt": _fmt_signed(q_inc - q_exp),
        })
    
    return {
        # Период
        "year": year,
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        
        # Итоги за год
        "total_income": total_income,
        "total_income_fmt": _fmt(total_income),
        "total_expense": total_expense,
        "total_expense_fmt": _fmt(total_expense),
        "total_assets": total_assets,
        "total_assets_fmt": _fmt(total_assets),
        "delta": delta,
        "delta_fmt": _fmt_signed(delta),
        "delta_sign": "📈" if delta >= 0 else "⚠️",
        "savings_rate": savings_rate,
        
        # Помесячные данные
        "monthly_data": [
            {
                "month": int(r["month"]),
                "month_name": MONTH_NAMES.get(int(r["month"]), ""),
                "income": float(r.get("income") or 0),
                "income_fmt": _fmt(float(r.get("income") or 0)),
                "expense": float(r.get("expense") or 0),
                "expense_fmt": _fmt(float(r.get("expense") or 0)),
                "assets": float(r.get("assets") or 0),
                "assets_fmt": _fmt(float(r.get("assets") or 0)),
                "delta": float(r.get("income") or 0) - float(r.get("expense") or 0),
                "delta_fmt": _fmt_signed(float(r.get("income") or 0) - float(r.get("expense") or 0)),
            }
            for r in monthly
        ],
        
        # Квартальная разбивка
        "quarters": quarters,
        
        # Топ категорий
        "top_categories": [
            {
                "name": c["category"],
                "total": float(c["total"] or 0),
                "total_fmt": _fmt(float(c["total"] or 0)),
            }
            for c in top_cats
        ],
        
        # Многолетний тренд для графика
        "trend": [
            {
                "label": f"{MONTH_NAMES.get(r['month'],'')[:3]} {r['year']}",
                "income": float(r.get("income") or 0),
                "expense": float(r.get("expense") or 0),
            }
            for r in trend
        ],
    }


def build_comparative_data(uid: int, periods: list) -> dict:
    """
    Собирает данные для comparative.html шаблона.
    
    Args:
        periods: список из 2-3 элементов [{"year": 2026, "month": 3}, ...]
    """
    import database as db
    from config import MONTH_NAMES
    
    result = []
    for p in periods[:3]:
        y, m = p["year"], p["month"]
        s = db.get_monthly_summary(uid, y, m)
        top = db.get_top_categories(uid, y, m, limit=10)
        
        inc = float(s["total_income"] or 0)
        exp = float(s["total_expense"] or 0)
        delta = inc - exp
        
        result.append({
            "year": y,
            "month": m,
            "label": f"{MONTH_NAMES.get(m,'')[:3]} {y}",
            "total_income": inc,
            "total_income_fmt": _fmt(inc),
            "total_expense": exp,
            "total_expense_fmt": _fmt(exp),
            "delta": delta,
            "delta_fmt": _fmt_signed(delta),
            "delta_sign": "📈" if delta >= 0 else "⚠️",
            "top_categories": [
                {
                    "name": c["category"],
                    "total": float(c["total"] or 0),
                    "total_fmt": _fmt(float(c["total"] or 0)),
                }
                for c in top
            ],
        })
    
    # Если 2 периода — считаем дельту между ними
    comparison = None
    if len(result) == 2:
        p1, p2 = result[0], result[1]
        diff_expense = p2["total_expense"] - p1["total_expense"]
        diff_income = p2["total_income"] - p1["total_income"]
        comparison = {
            "expense_diff": diff_expense,
            "expense_diff_fmt": _fmt_signed(diff_expense),
            "expense_pct": _pct(abs(diff_expense), p1["total_expense"]) if p1["total_expense"] else 0,
            "income_diff": diff_income,
            "income_diff_fmt": _fmt_signed(diff_income),
            "income_pct": _pct(abs(diff_income), p1["total_income"]) if p1["total_income"] else 0,
        }
    
    return {
        "periods": result,
        "comparison": comparison,
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
    }


def build_networth_data(uid: int, year: int) -> dict:
    """
    Собирает данные для networth.html шаблона.
    
    Показывает: чистый капитал, движение активов, норму сбережений.
    """
    import database as db
    from config import MONTH_NAMES
    
    monthly = db.get_monthly_summary_year(uid, year)
    asset_cats = db.get_category_breakdown(uid, year=year, section="Движение активов")
    
    # Считаем кумулятивный капитал по месяцам
    cumulative = 0.0
    net_worth_trend = []
    for r in monthly:
        inc = float(r.get("income") or 0)
        exp = float(r.get("expense") or 0)
        cumulative += inc - exp
        net_worth_trend.append({
            "month": int(r["month"]),
            "month_name": MONTH_NAMES.get(int(r["month"]), ""),
            "delta": inc - exp,
            "delta_fmt": _fmt_signed(inc - exp),
            "cumulative": cumulative,
            "cumulative_fmt": _fmt(cumulative),
        })
    
    total_assets = sum(float(r.get("assets") or 0) for r in monthly)
    total_income = sum(float(r.get("income") or 0) for r in monthly)
    total_expense = sum(float(r.get("expense") or 0) for r in monthly)
    total_savings = total_income - total_expense
    savings_rate = _pct(max(0, total_savings), total_income) if total_income else 0
    
    final_net_worth = net_worth_trend[-1]["cumulative"] if net_worth_trend else 0
    
    return {
        # Период
        "year": year,
        "generated_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        
        # Итоговые показатели
        "total_assets": total_assets,
        "total_assets_fmt": _fmt(total_assets),
        "total_savings": total_savings,
        "total_savings_fmt": _fmt_signed(total_savings),
        "savings_rate": savings_rate,
        "net_worth": final_net_worth,
        "net_worth_fmt": _fmt(final_net_worth),
        "net_worth_sign": "📈" if final_net_worth >= 0 else "⚠️",
        
        # Динамика капитала
        "net_worth_trend": net_worth_trend,
        
        # Категории активов
        "asset_categories": [
            {
                "name": c["category"],
                "total": float(c["total"] or 0),
                "total_fmt": _fmt(float(c["total"] or 0)),
                "pct": _pct(float(c["total"] or 0), total_assets) if total_assets else 0,
            }
            for c in asset_cats
            if float(c.get("total") or 0) > 0
        ],
    }


# ══════════════════════════════════════════════════════════════════════════════
# Универсальный билдер (опционально)
# ══════════════════════════════════════════════════════════════════════════════

def build_report_data(report_type: str, uid: int, **kwargs) -> dict:
    """
    Универсальная функция для выбора билдера по типу отчёта.
    
    Args:
        report_type: monthly | quarterly | yearly | comparative | networth
        **kwargs: параметры для конкретного билдера
    """
    builders = {
        "monthly": lambda: build_monthly_data(uid, kwargs["year"], kwargs["month"]),
        "quarterly": lambda: build_quarterly_data(uid, kwargs["year"], kwargs["quarter"]),
        "yearly": lambda: build_yearly_data(uid, kwargs["year"]),
        "comparative": lambda: build_comparative_data(uid, kwargs["periods"]),
        "networth": lambda: build_networth_data(uid, kwargs["year"]),
    }
    
    if report_type not in builders:
        raise ValueError(f"Неизвестный тип отчёта: {report_type}")
    
    return builders[report_type]()