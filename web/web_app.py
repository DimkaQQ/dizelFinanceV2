# -*- coding: utf-8 -*-
"""
DizelFinance — Flask Web App v2
Обновления:
✅ Поддержка скриншотов при загрузке
✅ Редактируемые итоги в аналитике (API + DB)
✅ Динамические категории (добавление/удаление через API)
✅ Унификация структуры категорий (Портфель детей и т.д.)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import uuid
import logging
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, flash, send_file
)
import io

import database as db
from config import (
    FLASK_SECRET, FLASK_PORT, WEB_USERS,
    SECTIONS, ALL_CATEGORIES, CURRENCIES,
    CURRENCY_SYMBOLS, MONTH_NAMES,
)
from rates import get_rate

log = logging.getLogger(__name__)
app = Flask(__name__)
app.secret_key = FLASK_SECRET

# Хранилище сессий загруженных файлов (in-memory)
upload_sessions: dict = {}
DEFAULT_USER = 0

def uid():
    """Возвращает user_id из сессии или дефолт."""
    user_id = session.get("user_id")
    if user_id is not None:
        return int(user_id)
    return DEFAULT_USER

def now():
    return datetime.now()

@app.context_processor
def inject_now():
    return {"now": now()}

# ── Auth ───────────────────────────────────────────────────────────────────────

@app.before_request
def require_login():
    public = ("login", "static", "health")
    if request.endpoint in public:
        return
    if not session.get("logged_in"):
        return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        
        if WEB_USERS.get(username) == password:
            session["logged_in"] = True
            session["username"] = username
            try:
                session["user_id"] = int(username)
                log.info(f"🔐 Web login: {username} → user_id={session['user_id']}")
            except ValueError:
                session["user_id"] = DEFAULT_USER
                log.warning(f"⚠️ Web login: {username} is not numeric, using DEFAULT_USER")
            return redirect(url_for("dashboard"))
        flash("Неверные данные")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    n     = now()
    u     = uid()
    data  = db.get_monthly_summary(u, n.year, n.month)
    trend = db.get_yearly_trend(u, years=2)
    top   = db.get_top_categories(u, n.year, n.month, limit=6)
    recent= db.get_transactions(u, limit=8)

    monthly_map = {}
    for r in trend:
        monthly_map[(int(r["year"]), int(r["month"]))] = r

    chart_labels, chart_income, chart_expense = [], [], []
    years_set = sorted({int(r["year"]) for r in trend})
    for y in years_set:
        for m in range(1, 13):
            row = monthly_map.get((y, m))
            if row:
                chart_labels.append(f"{MONTH_NAMES.get(m,'')[:3]} {y}")
                chart_income.append(float(row.get("income") or 0))
                chart_expense.append(float(row.get("expense") or 0))

    expense_sections = [(r["section"], float(r.get("expense") or 0))
                        for r in data["sections"] if float(r.get("expense") or 0) > 0]
    section_labels = [s[0] for s in expense_sections]
    section_values = [s[1] for s in expense_sections]

    return render_template(
        "dashboard.html",
        summary=data,
        month_name=MONTH_NAMES.get(n.month, ""),
        top_cats=top,
        recent_txs=recent,
        chart_labels=chart_labels,
        chart_income=chart_income,
        chart_expense=chart_expense,
        section_labels=section_labels,
        section_values=section_values,
    )

# ── Transactions ───────────────────────────────────────────────────────────────

@app.route("/transactions")
def transactions():
    u      = uid()
    page   = int(request.args.get("page", 1))
    per    = 30
    offset = (page - 1) * per
    year   = request.args.get("year",    type=int)
    month  = request.args.get("month",   type=int)
    section= request.args.get("section", "")

    total   = db.count_transactions(u, year=year, month=month)
    records = db.get_transactions(u, limit=per, offset=offset,
                                  year=year, month=month,
                                  section=section or None)
    for r in records:
        d = r.get("date")
        r["date_str"] = d.strftime("%d.%m.%Y") if hasattr(d, "strftime") else str(d)[:10]
        r["sym"]      = CURRENCY_SYMBOLS.get(r.get("currency", "RUB"), "₽")

    n     = now()
    years = list(range(n.year, n.year - 5, -1))
    return render_template(
        "transactions.html",
        records=records,
        page=page, per=per, total=total,
        pages=(total + per - 1) // per,
        year=year, month=month, section=section,
        sections=list(SECTIONS.keys()),
        years=years, months=MONTH_NAMES,
    )

@app.route("/transactions/add", methods=["GET", "POST"])
def add_transaction():
    if request.method == "POST":
        if request.form.get("quick_expense"):
            return quick_monthly_expense_post()
        
        u    = uid()
        cur  = request.form.get("currency", "RUB")
        amt  = float(request.form.get("amount", 0))
        rate = get_rate(cur)
        a_rub= round(amt * rate, 2)
        cat  = request.form.get("category", "")
        sec  = request.form.get("section", "")
        
        # Поддержка кастомных категорий: если нет в ALL_CATEGORIES, берём из БД
        if cat and cat not in ALL_CATEGORIES:
            db_cats = db.get_categories_for_section(u, sec)
            if cat not in db_cats:
                flash("❌ Неизвестная категория")
                return redirect(url_for("add_transaction"))
        
        tx_type_map = {"Доходы": "Доход", "Движение активов": "Актив"}
        tx_type = tx_type_map.get(sec, "Расход")
        
        db.save_transaction(u, {
            "date":       request.form.get("date", now().strftime("%d.%m.%Y")),
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
    
    n = now()
    return render_template(
        "add_transaction.html",
        sections=SECTIONS,
        currencies=CURRENCIES,
        today=n.strftime("%Y-%m-%d"),
        months=MONTH_NAMES,
        now=n,
    )

def quick_monthly_expense_post():
    u = uid()
    year = int(request.form.get("year", now().year))
    month = int(request.form.get("month", now().month))
    amount = float(request.form.get("quick_amount", 0))
    
    if amount <= 0:
        flash("❌ Введите сумму больше 0")
        return redirect(url_for("add_transaction"))
    
    month_name = MONTH_NAMES.get(month, "")
    db.save_transaction(u, {
        "date": f"01.{month:02d}.{year}",
        "section": "Регулярные расходы",
        "category": "Прочее (рег)",
        "amount": amount,
        "currency": "RUB",
        "rate": 1.0,
        "amount_rub": amount,
        "tx_type": "Расход",
        "merchant": f"Общий расход за {month_name} {year}",
        "comment": "Быстрый ввод (без детализации)",
        "source": "web_quick",
    })
    flash(f"✅ Записано {amount:,.0f} ₽ за {month_name} {year}")
    return redirect(url_for("transactions"))

@app.route("/transactions/<int:tx_id>/delete", methods=["POST"])
def delete_transaction(tx_id: int):
    db.delete_transaction(tx_id, uid())
    flash("🗑 Транзакция удалена.")
    return redirect(url_for("transactions"))

# ── Analytics ─────────────────────────────────────────────────────────────────

@app.route("/analytics")
def analytics():
    u     = uid()
    n     = now()
    year  = request.args.get("year",  n.year,  type=int)
    month = request.args.get("month", None,    type=int)
    cats  = db.get_category_breakdown(u, year=year, month=month)
    trend = db.get_yearly_trend(u, years=3)
    years = list(range(n.year, n.year - 5, -1))

    by_section = {}
    for row in cats:
        sec = row.get("section", "")
        by_section.setdefault(sec, []).append(row)

    # 🔥 Получаем корректировки категорий
    adjusted_categories = db.get_category_adjustments_for_period(u, year, month)
    
    conn = db.get_conn()
    cur = conn.cursor()
    
    # 1. Доходы за выбранный год (с учетом корректировок)
    if month:
        cur.execute("""
            SELECT COALESCE(SUM(amount_rub), 0) 
            FROM transactions 
            WHERE user_id = %s AND tx_type = 'Доход' 
            AND EXTRACT(YEAR FROM date) = %s 
            AND EXTRACT(MONTH FROM date) = %s
        """, (u, year, month))
    else:
        cur.execute("""
            SELECT COALESCE(SUM(amount_rub), 0) 
            FROM transactions 
            WHERE user_id = %s AND tx_type = 'Доход' 
            AND EXTRACT(YEAR FROM date) = %s
        """, (u, year))
    income = cur.fetchone()[0]
    
    # 2. Расходы за выбранный год/месяц (с учетом корректировок)
    if month:
        cur.execute("""
            SELECT category, SUM(amount_rub) as total
            FROM transactions 
            WHERE user_id = %s AND tx_type = 'Расход' 
            AND EXTRACT(YEAR FROM date) = %s 
            AND EXTRACT(MONTH FROM date) = %s
            GROUP BY category
        """, (u, year, month))
    else:
        cur.execute("""
            SELECT category, SUM(amount_rub) as total
            FROM transactions 
            WHERE user_id = %s AND tx_type = 'Расход' 
            AND EXTRACT(YEAR FROM date) = %s
            GROUP BY category
        """, (u, year))
    
    # Применяем корректировки к расходам
    expense = 0
    for row in cur.fetchall():
        category = row[0]
        original_amount = float(row[1])
        # Если есть корректировка для этой категории — используем её
        if category in adjusted_categories:
            expense += adjusted_categories[category]['value']
        else:
            expense += original_amount
    
    # 3. Дельта
    delta = income - expense
    
    # 4. Savings Rate
    savings_rate = (delta / income * 100) if income > 0 else 0
    
    # 5. Среднемесячные расходы
    months_count = 1 if month else 12
    avg_monthly = expense / months_count if expense > 0 else 0
    
    # 6. Чистый капитал (сумма всех активов на текущий момент)
    cur.execute("""
        SELECT COALESCE(SUM(amount_rub), 0) 
        FROM transactions 
        WHERE user_id = %s AND tx_type = 'Актив'
    """, (u,))
    net_worth = cur.fetchone()[0]
    
    # 7. Расходы за прошлый год (для инфляции)
    cur.execute("""
        SELECT COALESCE(SUM(amount_rub), 0) 
        FROM transactions 
        WHERE user_id = %s AND tx_type = 'Расход' 
        AND EXTRACT(YEAR FROM date) = %s
    """, (u, year - 1))
    expense_prev_year = cur.fetchone()[0]
    
    # 8. Личная инфляция
    inflation = ((expense / expense_prev_year - 1) * 100) if expense_prev_year > 0 else None
    
    # 9. Капитал на месяцы
    capital_months = round(net_worth / avg_monthly, 1) if avg_monthly > 0 else None
    
    cur.close()
    conn.close()
    
    # Формируем метрики с реальными значениями
    key_metrics = [
        {"name": "Доходы (₽/год)", "formula": "Сумма всех транзакций с 'Доход' за год", "unit": "₽", "value": round(income, 2)},
        {"name": "Расходы (₽/год)", "formula": "Сумма всех транзакций с 'Расход' за год (с учетом корректировок)", "unit": "₽", "value": round(expense, 2)},
        {"name": "Дельта / Cash Flow (₽)", "formula": "Доходы − Расходы", "unit": "₽", "value": round(delta, 2)},
        {"name": "Savings Rate (%)", "formula": "(Дельта / Доходы) × 100%", "unit": "%", "value": round(savings_rate, 2)},
        {"name": "Среднемесячные расходы", "formula": "Расходы за год ÷ 12", "unit": "₽", "value": round(avg_monthly, 2)},
        {"name": "Личная инфляция (%)", "formula": "(Расходы_тек/Расходы_пред − 1) × 100%", "unit": "%", "value": round(inflation, 2) if inflation is not None else None},
        {"name": "Чистый капитал (₽)", "formula": "Сумма активов − обязательства", "unit": "₽", "value": round(net_worth, 2)},
        {"name": "Рост капитала (%)", "formula": "(Капитал_тек/Капитал_пред − 1) × 100%", "unit": "%", "value": None},
        {"name": "FI Ratio (%)", "formula": "(Пассивный доход / Расходы) × 100%", "unit": "%", "value": None},
        {"name": "Капитал на месяцы", "formula": "Чистый капитал / Среднемесячные расходы", "unit": "мес", "value": capital_months},
    ]

    return render_template(
        "analytics.html",
        year=year, month=month,
        by_section=by_section,
        sections=SECTIONS,
        months=MONTH_NAMES,
        years=years,
        trend=trend,
        key_metrics=key_metrics,
        adjusted_categories=adjusted_categories,  # ← Передаем в шаблон
    )

# ── Upload file (с поддержкой скриншотов) ──────────────────────────────────────

@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    n     = now()
    years = list(range(n.year, n.year - 5, -1))

    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename:
            flash("Выберите файл")
            return redirect(url_for("upload_file"))

        fname    = f.filename.lower()
        raw      = f.read()
        u        = uid()
        year_sel = int(request.form.get("period_year", n.year))
        month_sel= int(request.form.get("period_month", n.month))

        try:
            from ai import parse_xlsx, parse_pdf, parse_screenshot, guess_categories_batch
            from txt_parser import parse_txt, read_txt_file
            from rates import get_rate

            txs = []
            # 🔥 Поддержка скриншотов
            if fname.endswith((".png", ".jpg", ".jpeg", ".webp")):
                mime = f"image/{fname.split('.')[-1]}"
                txs = parse_screenshot(raw, mime_type=mime)
            elif fname.endswith((".xlsx", ".xls")):
                txs = parse_xlsx(raw)
            elif fname.endswith(".pdf"):
                txs = parse_pdf(raw)
            elif fname.endswith(".txt"):
                text = read_txt_file(raw)
                txs  = parse_txt(text)
            else:
                flash("Неподдерживаемый формат файла. Используйте: Excel, PDF, TXT, PNG, JPG")
                return redirect(url_for("upload_file"))

            if not txs:
                flash("Транзакции не найдены в файле")
                return redirect(url_for("upload_file"))

            cat_results = guess_categories_batch(txs)
            existing    = db.get_existing_keys(u)
            enriched    = []

            for tx, (cat, sec) in zip(txs, cat_results):
                cur   = tx.get("currency", "RUB")
                rate  = get_rate(cur)
                a     = float(tx.get("amount", 0))
                a_rub = round(a * rate, 2)
                date_part = str(tx.get("date", "")).split(",")[0].strip()
                is_dup = f"{date_part}|{round(a_rub,2)}" in existing
                enriched.append({
                    **tx,
                    "category":     cat,
                    "section":      sec,
                    "rate":         rate,
                    "amount_rub":   a_rub,
                    "is_duplicate": is_dup,
                })

            sess_id = str(uuid.uuid4())[:8]
            upload_sessions[sess_id] = {"transactions": enriched, "user_id": u}

            result = {
                "session_id":   sess_id,
                "count":        len(enriched),
                "duplicates":   sum(1 for t in enriched if t["is_duplicate"]),
                "transactions": enriched,
            }
            return render_template("upload_file.html",
                                   result=result, years=years,
                                   months=MONTH_NAMES)

        except Exception as e:
            log.error(f"upload: {e}")
            flash(f"Ошибка обработки файла: {e}")
            return redirect(url_for("upload_file"))

    return render_template("upload_file.html", result=None,
                           years=years, months=MONTH_NAMES)

@app.route("/upload/confirm", methods=["POST"])
def upload_confirm():
    sess_id = request.form.get("session_id", "")
    action  = request.form.get("action", "all")
    sess    = upload_sessions.pop(sess_id, None)

    if not sess:
        flash("Сессия устарела, загрузите файл заново")
        return redirect(url_for("upload_file"))

    txs = sess["transactions"]
    u   = sess["user_id"]

    if action == "new_only":
        txs = [t for t in txs if not t["is_duplicate"]]

    saved = db.save_transactions_batch(u, [{**t, "source": "web_upload"} for t in txs])
    flash(f"✅ Записано {saved} транзакций!")
    return redirect(url_for("transactions"))

# ── Reports ────────────────────────────────────────────────────────────────────

@app.route("/reports")
def reports():
    n     = now()
    years = list(range(n.year, n.year - 5, -1))
    return render_template("reports.html", years=years, months=MONTH_NAMES)

@app.route("/reports/generate", methods=["POST"])
def generate_report():
    from pdf_caller import (
        generate_pdf,
        build_monthly_data, build_quarterly_data,
        build_yearly_data, build_networth_data, build_comparative_data,
    )

    template = request.form.get("template", "monthly")
    u        = uid()
    n        = now()

    try:
        if template == "monthly":
            year  = int(request.form.get("year",  n.year))
            month = int(request.form.get("month", n.month))
            data  = build_monthly_data(u, year, month)
            fname = f"DizelFinance_{MONTH_NAMES.get(month,'')}_{year}.pdf"
        elif template == "quarterly":
            year    = int(request.form.get("year",    n.year))
            quarter = int(request.form.get("quarter", 1))
            data    = build_quarterly_data(u, year, quarter)
            fname   = f"DizelFinance_Q{quarter}_{year}.pdf"
        elif template == "yearly":
            year  = int(request.form.get("year", n.year))
            data  = build_yearly_data(u, year)
            fname = f"DizelFinance_{year}_Годовой.pdf"
        elif template == "networth":
            year  = int(request.form.get("year", n.year))
            data  = build_networth_data(u, year)
            fname = f"DizelFinance_{year}_Капитал.pdf"
        elif template == "comparative":
            periods = []
            for i in range(1, 4):
                y = request.form.get(f"year_{i}",  type=int)
                m = request.form.get(f"month_{i}", type=int)
                if y and m:
                    periods.append({"year": y, "month": m})
            data  = build_comparative_data(u, periods)
            fname = "DizelFinance_Сравнение.pdf"
        else:
            flash("Неизвестный шаблон")
            return redirect(url_for("reports"))

        pdf_bytes = generate_pdf(template, data)
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=fname,
        )
    except RuntimeError as e:
        flash(f"❌ {e}")
        return redirect(url_for("reports"))
    except Exception as e:
        log.error(f"generate_report: {e}")
        flash(f"❌ Ошибка генерации: {e}")
        return redirect(url_for("reports"))

# ── API (Корректировки, Категории, Статус) ─────────────────────────────────────

@app.route("/api/summary")
def api_summary():
    n     = now()
    year  = request.args.get("year",  n.year,  type=int)
    month = request.args.get("month", n.month, type=int)
    return jsonify(db.get_monthly_summary(uid(), year, month))

@app.route("/api/categories", methods=["GET", "POST"])
def manage_categories():
    u = uid()
    if request.method == "POST":
        data = request.get_json()
        action = data.get("action")
        name = data.get("name", "").strip()
        section = data.get("section", "")
        if not name or not section:
            return jsonify({"error": "name и section обязательны"}), 400
        
        if action == "add":
            db.add_custom_category(u, name, section)
            return jsonify({"ok": True, "message": "Категория добавлена"})
        elif action == "remove":
            if db.remove_custom_category(u, name):
                return jsonify({"ok": True, "message": "Категория удалена"})
            return jsonify({"error": "Категория не найдена"}), 404
        return jsonify({"error": "Неизвестное действие"}), 400
    
    # GET: возвращаем объединённый список (кастомные + дефолтные)
    section = request.args.get("section", "")
    cats = db.get_categories_for_section(u, section)
    return jsonify(cats)

@app.route("/api/adjust-total", methods=["POST"])
def adjust_total():
    """POST: {year, month, metric_type, value, note}"""
    u = uid()
    data = request.get_json()
    if not data or not all(k in data for k in ["year", "metric_type", "value"]):
        return jsonify({"error": "Недостаточно данных"}), 400
    
    db.save_adjusted_total(
        u, 
        data["year"], 
        data.get("month"), 
        data["metric_type"], 
        float(data["value"]),
        data.get("note", "")
    )
    return jsonify({"ok": True, "message": "Итог обновлён"})

@app.route("/api/pdf-status")
def api_pdf_status():
    import requests as req
    try:
        r = req.get(os.getenv("PDF_SERVICE_URL", "http://localhost:3001") + "/health", timeout=3)
        return jsonify({"ok": r.status_code == 200})
    except Exception:
        return jsonify({"ok": False})

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "2.1"})

@app.route("/api/adjust-category", methods=["POST"])
def adjust_category():
    """POST: {year, month, category, value, note}"""
    u = uid()
    data = request.get_json()
    if not data or not all(k in data for k in ["year", "category", "value"]):
        return jsonify({"error": "Недостаточно данных"}), 400
    
    db.save_category_adjustment(
        u, 
        data["year"], 
        data.get("month"), 
        data["category"], 
        float(data["value"]),
        data.get("note", "")
    )
    return jsonify({"ok": True, "message": "Категория обновлена"})

# ── Миграция (одноразовый запуск) ────────────────────────────────────────────

@app.route("/migrate/portfolios")
def migrate_portfolios():
    """Замените 'admin' на реальный логин для доступа"""
    if session.get("username") != "admin":
        return jsonify({"error": "Доступ запрещён"}), 403
    u = uid()
    updated = db.migrate_portfolio_names(u)
    return jsonify({"ok": True, "updated_rows": updated, "message": f"Обновлено {updated} записей"})

# ── Run ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db.init_db()
    from config import WEB_FLASK_PORT
    log.info(f"🌐 Web App running on port {WEB_FLASK_PORT}")
    app.run(
        host="0.0.0.0",
        port=WEB_FLASK_PORT,
        debug=False,
        use_reloader=False
    )