# -*- coding: utf-8 -*-
"""
DizelFinance — клиент для PDF сервиса (Puppeteer)
Вызывается из bot.py и web/app.py
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

PDF_SERVICE_URL = os.getenv("PDF_SERVICE_URL", "http://localhost:3001")


def generate_pdf(template: str, data: dict) -> bytes:
    """
    Вызывает Node.js PDF сервис.
    template: monthly | quarterly | yearly | networth | comparative
    data: словарь с данными для шаблона
    Возвращает bytes PDF файла.
    """
    try:
        resp = requests.post(
            f"{PDF_SERVICE_URL}/generate",
            json={"template": template, "data": data},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.content
        else:
            raise RuntimeError(f"PDF сервис вернул {resp.status_code}: {resp.text[:200]}")
    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            "PDF сервис недоступен. Запусти: cd pdf_service && node server.js"
        )


def build_monthly_data(uid: int, year: int, month: int) -> dict:
    """Собирает все данные для monthly шаблона."""
    import database as db
    from config import MONTH_NAMES

    summary  = db.get_monthly_summary(uid, year, month)
    top_cats = db.get_top_categories(uid, year, month, limit=15)
    txs      = db.get_month_expenses_list(uid, year, month, limit=30, offset=0)
    trend    = db.get_yearly_trend(uid, years=3)

    # Данные прошлого месяца для сравнения
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    prev   = db.get_monthly_summary(uid, prev_y, prev_m)

    # Конвертируем даты в строки для JSON
    for tx in txs:
        if hasattr(tx.get("date"), "strftime"):
            tx["date"] = tx["date"].strftime("%d.%m.%Y")

    return {
        "year":          year,
        "month":         month,
        "prev_month":    prev_m,
        "prev_year":     prev_y,
        "total_income":  summary["total_income"],
        "total_expense": summary["total_expense"],
        "total_assets":  summary["total_assets"],
        "delta":         summary["delta"],
        "prev_income":   prev["total_income"],
        "prev_expense":  prev["total_expense"],
        "sections":      summary["sections"],
        "top_categories":top_cats,
        "transactions":  txs,
        "trend":         [
            {
                "year":    int(r["year"]),
                "month":   int(r["month"]),
                "income":  float(r.get("income")  or 0),
                "expense": float(r.get("expense") or 0),
            }
            for r in trend
        ],
    }


def build_quarterly_data(uid: int, year: int, quarter: int) -> dict:
    """Данные для квартального отчёта."""
    import database as db

    months = {1: [1,2,3], 2: [4,5,6], 3: [7,8,9], 4: [10,11,12]}
    q_months = months.get(quarter, [1,2,3])

    total_income = total_expense = total_assets = 0.0
    monthly_data = []
    all_txs = []

    for m in q_months:
        s = db.get_monthly_summary(uid, year, m)
        total_income  += s["total_income"]
        total_expense += s["total_expense"]
        total_assets  += s["total_assets"]
        monthly_data.append({
            "month":   m,
            "income":  s["total_income"],
            "expense": s["total_expense"],
            "delta":   s["delta"],
        })
        txs = db.get_month_expenses_list(uid, year, m, limit=100, offset=0)
        for tx in txs:
            if hasattr(tx.get("date"), "strftime"):
                tx["date"] = tx["date"].strftime("%d.%m.%Y")
        all_txs.extend(txs)

    # Топ категорий за квартал
    top_cats = db.get_top_categories(uid, year, limit=15)

    return {
        "year":           year,
        "quarter":        quarter,
        "total_income":   total_income,
        "total_expense":  total_expense,
        "total_assets":   total_assets,
        "delta":          total_income - total_expense,
        "monthly_data":   monthly_data,
        "top_categories": top_cats,
        "transactions":   all_txs[:50],
    }


def build_yearly_data(uid: int, year: int) -> dict:
    """Данные для годового отчёта."""
    import database as db

    monthly = db.get_monthly_summary_year(uid, year)
    top_cats = db.get_top_categories(uid, year, limit=20)
    trend    = db.get_yearly_trend(uid, years=3)

    total_income = sum(float(r.get("income") or 0) for r in monthly)
    total_expense = sum(float(r.get("expense") or 0) for r in monthly)
    total_assets  = sum(float(r.get("assets") or 0) for r in monthly)

    return {
        "year":           year,
        "total_income":   total_income,
        "total_expense":  total_expense,
        "total_assets":   total_assets,
        "delta":          total_income - total_expense,
        "monthly_data":   [
            {
                "month":   int(r["month"]),
                "income":  float(r.get("income")  or 0),
                "expense": float(r.get("expense") or 0),
                "assets":  float(r.get("assets")  or 0),
                "delta":   float(r.get("income") or 0) - float(r.get("expense") or 0),
            }
            for r in monthly
        ],
        "top_categories": top_cats,
        "trend":          [
            {
                "year":    int(r["year"]),
                "month":   int(r["month"]),
                "income":  float(r.get("income")  or 0),
                "expense": float(r.get("expense") or 0),
            }
            for r in trend
        ],
    }


def build_networth_data(uid: int, year: int) -> dict:
    """Данные для отчёта по активам и капиталу."""
    import database as db

    monthly = db.get_monthly_summary_year(uid, year)
    asset_cats = db.get_category_breakdown(uid, year=year, section="Движение активов")

    cumulative = 0.0
    net_worth_trend = []
    for r in monthly:
        cumulative += float(r.get("income") or 0) - float(r.get("expense") or 0)
        net_worth_trend.append({
            "month":     int(r["month"]),
            "delta":     float(r.get("income") or 0) - float(r.get("expense") or 0),
            "cumulative": cumulative,
        })

    total_assets  = sum(float(r.get("assets") or 0) for r in monthly)
    total_savings = sum(
        float(r.get("income") or 0) - float(r.get("expense") or 0)
        for r in monthly
    )

    return {
        "year":           year,
        "total_assets":   total_assets,
        "total_savings":  total_savings,
        "net_worth_trend":net_worth_trend,
        "asset_categories":asset_cats,
    }


def build_comparative_data(uid: int, periods: list) -> dict:
    """
    periods = [{"year":2026,"month":3}, {"year":2026,"month":2}, {"year":2026,"month":1}]
    """
    import database as db
    from config import MONTH_NAMES

    result = []
    for p in periods[:3]:
        y, m = p["year"], p["month"]
        s    = db.get_monthly_summary(uid, y, m)
        top  = db.get_top_categories(uid, y, m, limit=10)
        result.append({
            "year":          y,
            "month":         m,
            "label":         f"{MONTH_NAMES.get(m,'')[:3]} {y}",
            "total_income":  s["total_income"],
            "total_expense": s["total_expense"],
            "delta":         s["delta"],
            "top_categories":top,
        })

    return {"periods": result}
