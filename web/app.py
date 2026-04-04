# -*- coding: utf-8 -*-
"""
DizelFinance — Flask Web App
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, jsonify, flash
)
from datetime import datetime
import database as db
from config import (
    FLASK_SECRET, FLASK_PORT, WEB_USERNAME, WEB_PASSWORD,
    SECTIONS, ALL_CATEGORIES, CURRENCIES, CURRENCY_SYMBOLS,
    MONTH_NAMES,
)
from rates import get_rate

app = Flask(__name__)
app.secret_key = FLASK_SECRET

DEFAULT_USER = 0  # одиночный пользователь

def user_id():
    return session.get("user_id", DEFAULT_USER)

# ── Аутентификация ────────────────────────────────────────────────────────────

@app.before_request
def require_login():
    if request.endpoint in ("login", "static"):
        return
    if not session.get("logged_in"):
        return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if (request.form.get("username") == WEB_USERNAME and
                request.form.get("password") == WEB_PASSWORD):
            session["logged_in"] = True
            session["user_id"]   = DEFAULT_USER
            return redirect(url_for("dashboard"))
        flash("Неверные данные")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ── Dashboard ────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    now   = datetime.now()
    uid   = user_id()
    data  = db.get_monthly_summary(uid, now.year, now.month)
    trend = db.get_yearly_trend(uid, years=2)
    top   = db.get_top_categories(uid, now.year, now.month, limit=5)

    # Собрать данные для графика доходы/расходы по месяцам
    chart_labels = []
    chart_income = []
    chart_expense = []
    monthly_map: dict = {}
    for row in trend:
        key = (int(row["year"]), int(row["month"]))
        monthly_map[key] = row

    for y in sorted({int(r["year"]) for r in trend}):
        for m in range(1, 13):
            row = monthly_map.get((y, m))
            if row:
                chart_labels.append(f"{MONTH_NAMES.get(m,'')[:3]} {y}")
                chart_income.append(float(row.get("income") or 0))
                chart_expense.append(float(row.get("expense") or 0))

    return render_template(
        "dashboard.html",
        now=now,
        month_name=MONTH_NAMES.get(now.month, ""),
        summary=data,
        sections=SECTIONS,
        top_cats=top,
        chart_labels=chart_labels,
        chart_income=chart_income,
        chart_expense=chart_expense,
    )

# ── Транзакции ───────────────────────────────────────────────────────────────

@app.route("/transactions")
def transactions():
    uid    = user_id()
    page   = int(request.args.get("page", 1))
    per    = 30
    offset = (page - 1) * per
    year   = request.args.get("year", type=int)
    month  = request.args.get("month", type=int)
    section= request.args.get("section", "")
    total  = db.count_transactions(uid, year=year, month=month)
    records= db.get_transactions(uid, limit=per, offset=offset,
                                  year=year, month=month,
                                  section=section or None)
    # форматируем даты
    for r in records:
        d = r.get("date")
        if hasattr(d, "strftime"):
            r["date_str"] = d.strftime("%d.%m.%Y")
        else:
            r["date_str"] = str(d)
        r["sym"] = CURRENCY_SYMBOLS.get(r.get("currency","RUB"), "")

    now = datetime.now()
    years = list(range(now.year, now.year - 5, -1))
    return render_template(
        "transactions.html",
        records=records,
        page=page,
        per=per,
        total=total,
        pages=(total + per - 1) // per,
        year=year,
        month=month,
        section=section,
        sections=list(SECTIONS.keys()),
        years=years,
        months=MONTH_NAMES,
        currency_symbols=CURRENCY_SYMBOLS,
    )

@app.route("/transactions/add", methods=["GET", "POST"])
def add_transaction():
    if request.method == "POST":
        uid = user_id()
        cur = request.form.get("currency", "RUB")
        amt = float(request.form.get("amount", 0))
        rate = get_rate(cur)
        a_rub = round(amt * rate, 2)
        cat  = request.form.get("category", "")
        sec  = ALL_CATEGORIES.get(cat, "")
        tx_type_map = {
            "Доходы": "Доход",
            "Движение активов": "Актив",
        }
        tx_type = tx_type_map.get(sec, "Расход")
        db.save_transaction(uid, {
            "date":       request.form.get("date", datetime.now().strftime("%d.%m.%Y")),
            "section":    sec,
            "category":   cat,
            "amount":     amt,
            "currency":   cur,
            "rate":       rate,
            "amount_rub": a_rub,
            "tx_type":    tx_type,
            "merchant":   request.form.get("merchant", ""),
            "comment":    request.form.get("comment", ""),
            "source":     "web",
        })
        flash("✅ Транзакция добавлена!")
        return redirect(url_for("transactions"))
    now = datetime.now()
    return render_template(
        "add_transaction.html",
        sections=SECTIONS,
        currencies=CURRENCIES,
        today=now.strftime("%Y-%m-%d"),
    )

@app.route("/transactions/<int:tx_id>/delete", methods=["POST"])
def delete_transaction(tx_id: int):
    db.delete_transaction(tx_id, user_id())
    flash("🗑 Транзакция удалена.")
    return redirect(url_for("transactions"))

# ── Аналитика ────────────────────────────────────────────────────────────────

@app.route("/analytics")
def analytics():
    uid  = user_id()
    now  = datetime.now()
    year = request.args.get("year", now.year, type=int)
    month= request.args.get("month", None, type=int)
    cats = db.get_category_breakdown(uid, year=year, month=month)
    trend= db.get_yearly_trend(uid, years=3)
    years = list(range(now.year, now.year - 5, -1))

    # Группируем по разделам
    by_section: dict = {}
    for row in cats:
        sec = row.get("section","")
        if sec not in by_section:
            by_section[sec] = []
        by_section[sec].append(row)

    return render_template(
        "analytics.html",
        year=year,
        month=month,
        by_section=by_section,
        sections=SECTIONS,
        months=MONTH_NAMES,
        years=years,
        trend=trend,
    )

# ── API (для JS-запросов) ─────────────────────────────────────────────────────

@app.route("/api/summary")
def api_summary():
    uid   = user_id()
    now   = datetime.now()
    year  = request.args.get("year",  now.year,  type=int)
    month = request.args.get("month", now.month, type=int)
    return jsonify(db.get_monthly_summary(uid, year, month))

@app.route("/api/categories")
def api_categories():
    section = request.args.get("section", "")
    cats = SECTIONS.get(section, {}).get("categories", [])
    return jsonify(cats)

# ── Запуск ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db.init_db()
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False)
