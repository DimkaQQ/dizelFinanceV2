# -*- coding: utf-8 -*-
"""
DizelFinance Bot v3 — aiogram 3, PostgreSQL, полный дашборд
"""

import logging
import uuid
import asyncio
import io
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from voice import transcribe, transcribe_fallback
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from flask import Flask, request, jsonify
import threading
import requests as req

import database as db
from ai import (
    guess_category, guess_categories_batch,
    parse_screenshot, parse_pdf, parse_xlsx, parse_sms,
)
from rates import get_rate
from config import (
    TELEGRAM_TOKEN, ALLOWED_IDS, ADMIN_ID,
    CURRENCIES, CURRENCY_SYMBOLS,
    SECTIONS, ALL_CATEGORIES, MONTH_NAMES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

pending: dict = {}
pdf_sessions: dict = {}

# ══════════════════════════════════════════════════════════════════════════════
# FSM
# ══════════════════════════════════════════════════════════════════════════════

class TxForm(StatesGroup):
    tx_type       = State()
    section       = State()
    category      = State()
    amount        = State()
    currency      = State()
    date          = State()
    confirm       = State()
    edit_amount   = State()
    edit_currency = State()

class PDFReview(StatesGroup):
    reviewing = State()

class CompareForm(StatesGroup):
    pick_month1 = State()
    pick_month2 = State()

# ══════════════════════════════════════════════════════════════════════════════
# Клавиатуры
# ══════════════════════════════════════════════════════════════════════════════

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить транзакцию")],
        [KeyboardButton(text="📊 Дашборд"),    KeyboardButton(text="📈 Аналитика")],
        [KeyboardButton(text="📋 Транзакции"), KeyboardButton(text="📥 Черновики")],
        [KeyboardButton(text="⚙️ Настройки")],
    ], resize_keyboard=True)

def kb_analytics() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📊 За этот месяц"), KeyboardButton(text="📅 Выбрать месяц")],
        [KeyboardButton(text="📈 Сравнить месяцы"), KeyboardButton(text="🏆 Топ-10 категорий")],
        [KeyboardButton(text="💸 Все расходы"), KeyboardButton(text="📄 Выписка PDF")],
        [KeyboardButton(text="⏪ Главное меню")],
    ], resize_keyboard=True)

def kb_tx_type() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💸 Расход"),  KeyboardButton(text="💰 Доход")],
        [KeyboardButton(text="🏦 Актив")],
        [KeyboardButton(text="⏪ Назад")],
    ], resize_keyboard=True, one_time_keyboard=True)

def kb_section(tx_type: str) -> ReplyKeyboardMarkup:
    rows = []
    if tx_type == "Доход":
        rows.append([KeyboardButton(text="💰 Доходы")])
    elif tx_type == "Актив":
        rows.append([KeyboardButton(text="🏦 Движение активов")])
    else:
        rows.append([KeyboardButton(text="🛒 Регулярные расходы")])
        rows.append([KeyboardButton(text="💳 Крупные траты")])
    rows.append([KeyboardButton(text="⏪ Назад")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

SECTION_LABEL = {
    "💰 Доходы":             "Доходы",
    "🏦 Движение активов":   "Движение активов",
    "🛒 Регулярные расходы": "Регулярные расходы",
    "💳 Крупные траты":      "Крупные траты",
}

def kb_categories(section: str) -> ReplyKeyboardMarkup:
    cats = SECTIONS.get(section, {}).get("categories", [])
    rows = []
    for i in range(0, len(cats), 2):
        row = [KeyboardButton(text=cats[i])]
        if i + 1 < len(cats):
            row.append(KeyboardButton(text=cats[i+1]))
        rows.append(row)
    rows.append([KeyboardButton(text="⏪ Назад")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def kb_currencies() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="RUB"), KeyboardButton(text="USD"), KeyboardButton(text="EUR")],
        [KeyboardButton(text="KZT"), KeyboardButton(text="IDR"), KeyboardButton(text="VND")],
        [KeyboardButton(text="⏪ Назад")],
    ], resize_keyboard=True, one_time_keyboard=True)

def kb_back() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⏪ Назад")]
    ], resize_keyboard=True, one_time_keyboard=True)

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ Записать")],
        [KeyboardButton(text="✏️ Изменить категорию"), KeyboardButton(text="🔢 Изменить сумму")],
        [KeyboardButton(text="❌ Отменить")],
    ], resize_keyboard=True, one_time_keyboard=True)

def kb_pdf_action() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Записать все", callback_data="pdf|all"),
            InlineKeyboardButton(text="👀 По одной",    callback_data="pdf|review"),
        ],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="pdf|cancel")],
    ])

def kb_pdf_item(idx: int, total: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="✅ Записать",   callback_data=f"pi|save|{idx}"),
            InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"pi|skip|{idx}"),
        ],
        [
            InlineKeyboardButton(text="✏️ Категория", callback_data=f"pi|edit_cat|{idx}"),
            InlineKeyboardButton(text="🔢 Сумма",     callback_data=f"pi|edit_amt|{idx}"),
        ],
    ]
    if idx + 1 < total:
        rows.append([InlineKeyboardButton(
            text=f"➡️ Следующая ({idx+2}/{total})",
            callback_data=f"pi|next|{idx}"
        )])
    else:
        rows.append([InlineKeyboardButton(text="🏁 Завершить", callback_data="pi|done|0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_quick_cats(tx_id: str, category: str, section: str) -> InlineKeyboardMarkup:
    cats = SECTIONS.get(section, {}).get("categories", [])
    alts = [c for c in cats if c != category][:3]
    rows = [[InlineKeyboardButton(
        text=f"✅ {category}",
        callback_data=f"qc|{tx_id}|{category}|{section}"
    )]]
    for alt in alts:
        rows.append([InlineKeyboardButton(text=alt, callback_data=f"qc|{tx_id}|{alt}|{section}")])
    rows.append([
        InlineKeyboardButton(text="📋 Все категории", callback_data=f"qa|{tx_id}"),
        InlineKeyboardButton(text="❌ Пропустить",    callback_data="qn|skip"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_month_picker(prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора месяца (текущий год + прошлый)."""
    now   = datetime.now()
    rows  = []
    months_row = []
    for m in range(1, 13):
        months_row.append(InlineKeyboardButton(
            text=MONTH_NAMES[m][:3],
            callback_data=f"{prefix}|{now.year}|{m}"
        ))
        if len(months_row) == 3:
            rows.append(months_row)
            months_row = []
    if months_row:
        rows.append(months_row)
    # Прошлый год
    prev_year = now.year - 1
    rows.append([InlineKeyboardButton(
        text=f"── {prev_year} ──", callback_data="noop"
    )])
    py_row = []
    for m in range(1, 13):
        py_row.append(InlineKeyboardButton(
            text=MONTH_NAMES[m][:3],
            callback_data=f"{prefix}|{prev_year}|{m}"
        ))
        if len(py_row) == 3:
            rows.append(py_row)
            py_row = []
    if py_row:
        rows.append(py_row)
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_pick")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_expenses_nav(year: int, month: int, offset: int,
                     total: int, per: int = 10) -> InlineKeyboardMarkup:
    rows = []
    nav  = []
    if offset > 0:
        nav.append(InlineKeyboardButton(
            text="◀️ Назад", callback_data=f"exp|{year}|{month}|{offset-per}"
        ))
    if offset + per < total:
        nav.append(InlineKeyboardButton(
            text="Вперёд ▶️", callback_data=f"exp|{year}|{month}|{offset+per}"
        ))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="close_exp")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ══════════════════════════════════════════════════════════════════════════════
# Превью транзакции
# ══════════════════════════════════════════════════════════════════════════════

def build_preview(data: dict) -> str:
    cur        = data.get("currency", "RUB")
    amount     = float(data.get("amount", 0))
    rate       = float(data.get("rate", 1.0))
    amount_rub = float(data.get("amount_rub", amount))
    sym        = CURRENCY_SYMBOLS.get(cur, cur)
    tx_type    = data.get("tx_type", "Расход")
    icon       = "💰" if tx_type == "Доход" else ("🏦" if tx_type == "Актив" else "💸")
    merchant   = data.get("merchant", "")

    text = (
        f"📝 <b>Предварительный просмотр</b>\n\n"
        f"{icon} <b>{tx_type}</b>\n"
        f"📅 {data.get('date', '')}\n"
        f"📂 {data.get('section', '')} → <b>{data.get('category', '')}</b>\n"
    )
    if merchant:
        text += f"🏪 {merchant}\n"
    text += "\n"
    if cur == "RUB":
        text += f"💰 <code>{amount:,.0f} ₽</code>\n"
    else:
        text += (
            f"💰 <code>{amount:,.2f} {sym}</code>\n"
            f"🔄 <code>{amount_rub:,.0f} ₽</code> (курс {rate:,.4f})\n"
        )
    return text

def build_pdf_preview(tx: dict, idx: int, total: int) -> str:
    cur  = tx.get("currency", "RUB")
    sym  = CURRENCY_SYMBOLS.get(cur, cur)
    icon = "💰" if tx.get("tx_type") == "Доход" else "💸"
    text = (
        f"<b>{idx+1} / {total}</b>\n\n"
        f"{icon} <b>{tx.get('merchant', '—')}</b>\n"
        f"💰 <code>{float(tx.get('amount',0)):,.2f} {sym}</code>\n"
        f"📅 {tx.get('date','—')}\n"
        f"📂 {tx.get('section','—')} → <b>{tx.get('category','—')}</b>"
    )
    if tx.get("category_hint"):
        text += f"\n💡 <i>{tx['category_hint']}</i>"
    if tx.get("is_duplicate"):
        text += "\n⚠️ <b>Возможный дубликат</b>"
    return text

# ══════════════════════════════════════════════════════════════════════════════
# Дашборд — генерация текста
# ══════════════════════════════════════════════════════════════════════════════

def _bar(pct: float, width: int = 12) -> str:
    filled = int(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)

def build_dashboard(uid: int, year: int, month: int) -> str:
    data       = db.get_monthly_summary(uid, year, month)
    month_name = MONTH_NAMES.get(month, "")
    top_cats   = db.get_top_categories(uid, year, month, limit=5)

    # Сравнение с прошлым месяцем
    prev_month = month - 1 if month > 1 else 12
    prev_year  = year if month > 1 else year - 1
    prev_data  = db.get_monthly_summary(uid, prev_year, prev_month)

    income  = data["total_income"]
    expense = data["total_expense"]
    assets  = data["total_assets"]
    delta   = data["delta"]

    prev_exp = prev_data["total_expense"]
    exp_diff = expense - prev_exp
    exp_pct  = round(exp_diff / prev_exp * 100, 1) if prev_exp else 0
    exp_vs   = f"{'📈' if exp_diff > 0 else '📉'} {exp_pct:+.1f}% vs {MONTH_NAMES.get(prev_month,'')[:3]}"

    text = (
        f"📊 <b>Дашборд — {month_name} {year}</b>\n"
        f"{'─' * 30}\n\n"
        f"💚 Доходы:   <b>{income:>12,.0f} ₽</b>\n"
        f"❤️  Расходы: <b>{expense:>12,.0f} ₽</b>  {exp_vs}\n"
    )
    if assets:
        text += f"🏦 Активы:   <b>{assets:>12,.0f} ₽</b>\n"
    delta_icon = "📈" if delta >= 0 else "⚠️"
    text += (
        f"{'─' * 30}\n"
        f"{delta_icon} Дельта:   <b>{delta:>+12,.0f} ₽</b>\n"
    )

    # Структура расходов по разделам
    expense_sections = [
        r for r in data["sections"]
        if float(r.get("expense") or 0) > 0
    ]
    if expense_sections and expense > 0:
        text += f"\n<b>Структура расходов:</b>\n"
        for row in sorted(expense_sections, key=lambda x: float(x.get("expense") or 0), reverse=True):
            sec  = row.get("section", "")
            exp  = float(row.get("expense") or 0)
            pct  = exp / expense * 100
            icon = SECTIONS.get(sec, {}).get("icon", "📂")
            text += f"{icon} {sec[:16]:<16} {_bar(pct, 8)} {pct:4.0f}%  {exp:,.0f} ₽\n"

    # Топ-5 категорий
    if top_cats:
        text += f"\n<b>Топ расходов:</b>\n"
        max_val = float(top_cats[0]["total"]) if top_cats else 1
        for i, c in enumerate(top_cats):
            total = float(c["total"])
            pct   = total / max_val * 100
            text += f"{i+1}. {c['category'][:18]:<18} <code>{total:>8,.0f} ₽</code>\n"

    return text

def build_compare(uid: int, year1: int, month1: int, year2: int, month2: int) -> str:
    cmp  = db.get_compare_months(uid, year1, month1, year2, month2)
    mn1  = f"{MONTH_NAMES.get(month1,'')[:3]} {year1}"
    mn2  = f"{MONTH_NAMES.get(month2,'')[:3]} {year2}"

    t1 = cmp["total1"]
    t2 = cmp["total2"]

    text = (
        f"📈 <b>Сравнение: {mn1} vs {mn2}</b>\n"
        f"{'─' * 32}\n\n"
        f"{'':20} {mn1:>10} {mn2:>10}  Δ\n"
        f"{'─' * 32}\n"
        f"💚 {'Доходы':<18} {t1['income']:>10,.0f} {t2['income']:>10,.0f}  "
        f"{t2['income']-t1['income']:>+8,.0f}\n"
        f"❤️  {'Расходы':<17} {t1['expense']:>10,.0f} {t2['expense']:>10,.0f}  "
        f"{t2['expense']-t1['expense']:>+8,.0f}\n"
        f"{'─' * 32}\n\n"
        f"<b>По разделам (расходы):</b>\n"
    )
    for row in cmp["sections"]:
        if row["expense1"] == 0 and row["expense2"] == 0:
            continue
        sec  = row["section"]
        icon = SECTIONS.get(sec, {}).get("icon", "📂")
        diff = row["exp_diff"]
        pct  = row["exp_pct"]
        arrow = "📈" if diff > 0 else ("📉" if diff < 0 else "➡️")
        text += (
            f"{icon} {sec[:14]:<14} "
            f"{row['expense1']:>8,.0f} → {row['expense2']:>8,.0f}  "
            f"{arrow}{pct:>+5.0f}%\n"
        )
    return text

def build_expenses_list(uid: int, year: int, month: int,
                         offset: int = 0, per: int = 10) -> tuple[str, int]:
    records = db.get_month_expenses_list(uid, year, month, limit=per, offset=offset)
    total   = db.count_month_expenses(uid, year, month)
    month_name = MONTH_NAMES.get(month, "")

    text = (
        f"💸 <b>Расходы — {month_name} {year}</b>\n"
        f"Показано {offset+1}–{min(offset+per, total)} из {total}\n"
        f"{'─' * 30}\n"
    )
    for r in records:
        cur  = r.get("currency", "RUB")
        sym  = CURRENCY_SYMBOLS.get(cur, cur)
        amt  = float(r.get("amount", 0))
        a_rub = float(r.get("amount_rub", amt))
        d    = r.get("date", "")
        if hasattr(d, "strftime"): d = d.strftime("%d.%m")
        merchant = r.get("merchant", "")
        cat  = r.get("category", "")
        text += f"<b>{d}</b> {cat}"
        if merchant:
            text += f" · <i>{merchant[:15]}</i>"
        text += f"\n   <code>{amt:,.0f} {sym}"
        if cur != "RUB":
            text += f" ({a_rub:,.0f} ₽)"
        text += "</code>\n"
    return text, total

# ══════════════════════════════════════════════════════════════════════════════
# Генерация PDF выписки
# ══════════════════════════════════════════════════════════════════════════════

def generate_pdf_report(uid: int, year: int, month: int) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except ImportError:
        raise ImportError("reportlab не установлен. Запусти: pip install reportlab")

    buf        = io.BytesIO()
    doc        = SimpleDocTemplate(buf, pagesize=A4)
    styles     = getSampleStyleSheet()
    story      = []
    month_name = MONTH_NAMES.get(month, "")

    # Заголовок
    title_style = ParagraphStyle("title", parent=styles["Title"], fontSize=16)
    story.append(Paragraph(f"DizelFinance — {month_name} {year}", title_style))
    story.append(Spacer(1, 12))

    # Сводка
    data_summary = db.get_monthly_summary(uid, year, month)
    summary_data = [
        ["Показатель", "Сумма (₽)"],
        ["💚 Доходы",  f"{data_summary['total_income']:,.0f}"],
        ["❤️ Расходы", f"{data_summary['total_expense']:,.0f}"],
        ["📊 Дельта",  f"{data_summary['delta']:+,.0f}"],
    ]
    if data_summary["total_assets"]:
        summary_data.append(["🏦 Активы", f"{data_summary['total_assets']:,.0f}"])

    t = Table(summary_data, colWidths=[200, 150])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#1a1a24")),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTSIZE",   (0,0), (-1,-1), 11),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("GRID",       (0,0), (-1,-1), 0.5, colors.grey),
        ("ALIGN",      (1,0), (1,-1), "RIGHT"),
    ]))
    story.append(t)
    story.append(Spacer(1, 16))

    # Топ категорий
    story.append(Paragraph("Топ категорий расходов", styles["Heading2"]))
    top = db.get_top_categories(uid, year, month, limit=10)
    if top:
        cat_data = [["Категория", "Сумма (₽)"]]
        for c in top:
            cat_data.append([c["category"], f"{float(c['total']):,.0f}"])
        t2 = Table(cat_data, colWidths=[250, 150])
        t2.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#6366f1")),
            ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
            ("FONTSIZE",   (0,0), (-1,-1), 10),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f0f0ff")]),
            ("GRID",       (0,0), (-1,-1), 0.5, colors.grey),
            ("ALIGN",      (1,0), (1,-1), "RIGHT"),
        ]))
        story.append(t2)
    story.append(Spacer(1, 16))

    # Все транзакции
    story.append(Paragraph("Все транзакции", styles["Heading2"]))
    records = db.get_month_expenses_list(uid, year, month, limit=500, offset=0)
    if records:
        tx_data = [["Дата", "Категория", "Место", "Сумма (₽)"]]
        for r in records:
            d = r.get("date", "")
            if hasattr(d, "strftime"): d = d.strftime("%d.%m.%Y")
            tx_data.append([
                str(d),
                r.get("category", ""),
                (r.get("merchant") or "")[:20],
                f"{float(r.get('amount_rub', r.get('amount',0))):,.0f}",
            ])
        t3 = Table(tx_data, colWidths=[70, 130, 130, 90])
        t3.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#374151")),
            ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
            ("FONTSIZE",   (0,0), (-1,-1), 8),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f9f9f9")]),
            ("GRID",       (0,0), (-1,-1), 0.3, colors.lightgrey),
            ("ALIGN",      (3,0), (3,-1), "RIGHT"),
        ]))
        story.append(t3)

    doc.build(story)
    return buf.getvalue()

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def allowed(user_id: int) -> bool:
    return not ALLOWED_IDS or user_id in ALLOWED_IDS

def _enrich(transactions: list, uid: int) -> list:
    existing    = db.get_existing_keys(uid)
    cat_results = guess_categories_batch(transactions)
    enriched    = []
    for tx, (cat, sec) in zip(transactions, cat_results):
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
    return enriched

def _store_session(uid: int, enriched: list):
    pdf_sessions[uid] = {
        "transactions":  enriched,
        "saved_count":   0,
        "skipped_count": 0,
    }

async def _send_summary(msg: Message, enriched: list, label: str):
    dup  = sum(1 for t in enriched if t["is_duplicate"])
    new_ = len(enriched) - dup
    await msg.answer(
        f"✅ <b>{label} обработан!</b>\n\n"
        f"📄 Найдено: <b>{len(enriched)}</b>\n"
        f"🆕 Новых: <b>{new_}</b>\n"
        f"⚠️ Дубликатов: <b>{dup}</b>\n\n"
        f"Что делаем?",
        reply_markup=kb_pdf_action()
    )

async def show_pdf_tx(msg: Message, uid: int, idx: int):
    sess = pdf_sessions.get(uid)
    if not sess:
        await msg.answer("❌ Сессия завершена.", reply_markup=kb_main())
        return
    txs = sess["transactions"]
    if idx >= len(txs):
        saved   = sess.get("saved_count", 0)
        skipped = sess.get("skipped_count", 0)
        pdf_sessions.pop(uid, None)
        await msg.answer(
            f"🏁 <b>Готово!</b>\n✅ Записано: {saved}\n⏭ Пропущено: {skipped}",
            reply_markup=kb_main()
        )
        return
    await msg.answer(
        build_pdf_preview(txs[idx], idx, len(txs)),
        reply_markup=kb_pdf_item(idx, len(txs))
    )

async def _send_single_tx(msg: Message, tx: dict):
    uid      = msg.from_user.id
    amount   = float(tx.get("amount", 0))
    cur      = tx.get("currency", "RUB")
    merchant = tx.get("merchant", "")
    tx_type  = tx.get("tx_type", "Расход")
    hint     = tx.get("category_hint", "")
    date     = tx.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M")
    rate     = get_rate(cur)
    a_rub    = round(amount * rate, 2)
    sym      = CURRENCY_SYMBOLS.get(cur, cur)
    icon     = "💰" if tx_type == "Доход" else "💸"

    cat, sec = guess_category(merchant, amount, tx_type=tx_type, hint=hint)
    tx_id = str(uuid.uuid4())[:8]
    pending[tx_id] = {
        "a": amount, "m": merchant, "d": date,
        "cur": cur, "rate": rate, "a_rub": a_rub,
        "tx_type": tx_type, "category": cat, "section": sec,
    }
    db.drafts_add(uid, {
        "id": str(uuid.uuid4())[:8], "a": amount, "m": merchant, "d": date,
        "cur": cur, "rate": rate, "a_rub": a_rub, "tx_type": tx_type,
    })
    sym_line  = f"{amount:,.0f} ₽" if cur == "RUB" else f"{amount:,.2f} {sym}"
    hint_line = f"\n💡 <i>{hint}</i>" if hint else ""
    await msg.answer(
        f"📸 <b>Распознано</b>\n\n"
        f"{icon} {tx_type} | <code>{sym_line}</code>\n"
        f"🏪 {merchant}\n"
        f"📅 {date}{hint_line}\n\n"
        f"🤖 <b>{sec} → {cat}</b>\n"
        f"Подтвердите или выберите другую:",
        reply_markup=kb_quick_cats(tx_id, cat, sec)
    )

def _notify_admin(text: str):
    if not ADMIN_ID: return
    try:
        req.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_ID, "text": text, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception as e:
        log.error(f"notify_admin: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id):
        await msg.answer("🔒 Нет доступа.")
        return
    await state.clear()
    await msg.answer(
        "👋 <b>DizelFinance v3</b>\n\n"
        "Личный финансовый трекер.\n\n"
        "<b>Как добавить транзакцию:</b>\n"
        "📸 Скриншот банка\n"
        "📄 PDF / Excel выписка\n"
        "💬 Текст SMS\n"
        "✏️ Кнопка «Добавить транзакцию»",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Главное меню
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "➕ Добавить транзакцию")
async def new_tx(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await state.clear()
    await state.set_state(TxForm.tx_type)
    await msg.answer("Тип операции:", reply_markup=kb_tx_type())

@dp.message(StateFilter("*"), F.text == "⏪ Главное меню")
async def go_main(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Главное меню:", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Дашборд
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "📊 Дашборд")
async def dashboard(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    now = datetime.now()
    await msg.answer("⏳ Загружаю дашборд...")
    text = build_dashboard(msg.from_user.id, now.year, now.month)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💸 Все расходы",    callback_data=f"exp|{now.year}|{now.month}|0"),
            InlineKeyboardButton(text="🏆 Топ-10",         callback_data=f"top|{now.year}|{now.month}"),
        ],
        [
            InlineKeyboardButton(text="📅 Другой месяц",  callback_data="pick_dash"),
            InlineKeyboardButton(text="📄 PDF выписка",   callback_data=f"rpdf|{now.year}|{now.month}"),
        ],
        [InlineKeyboardButton(text="📈 Сравнить месяцы", callback_data="start_compare")],
    ])
    await msg.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("top|"))
async def cb_top(cb: CallbackQuery):
    _, year, month = cb.data.split("|")
    uid  = cb.from_user.id
    cats = db.get_top_categories(uid, int(year), int(month), limit=10)
    if not cats:
        await cb.answer("Нет данных", show_alert=True)
        return
    month_name = MONTH_NAMES.get(int(month), "")
    total = sum(float(c["total"]) for c in cats)
    text  = f"🏆 <b>Топ-10 расходов — {month_name} {year}</b>\n\n"
    max_v = float(cats[0]["total"])
    for i, c in enumerate(cats):
        v   = float(c["total"])
        pct = v / total * 100
        bar = _bar(v / max_v * 100, 10)
        text += f"{i+1:2}. {c['category'][:20]:<20}\n    {bar} {pct:.1f}%  <code>{v:,.0f} ₽</code>\n"
    await cb.message.answer(text)
    await cb.answer()

@dp.callback_query(F.data.startswith("rpdf|"))
async def cb_report_pdf(cb: CallbackQuery):
    _, year, month = cb.data.split("|")
    uid = cb.from_user.id
    await cb.message.answer("⏳ Генерирую PDF выписку...")
    try:
        pdf_bytes  = generate_pdf_report(uid, int(year), int(month))
        month_name = MONTH_NAMES.get(int(month), "")
        filename   = f"DizelFinance_{month_name}_{year}.pdf"
        await bot.send_document(
            cb.from_user.id,
            document=BufferedInputFile(pdf_bytes, filename=filename),
            caption=f"📄 Выписка за {month_name} {year}"
        )
    except ImportError:
        await cb.message.answer(
            "❌ Для PDF нужен reportlab:\n<code>pip install reportlab</code>"
        )
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка генерации PDF: {e}")
    await cb.answer()

@dp.callback_query(F.data == "pick_dash")
async def cb_pick_dash(cb: CallbackQuery):
    await cb.message.answer(
        "Выберите месяц для дашборда:",
        reply_markup=kb_month_picker("dash")
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("dash|"))
async def cb_dash_month(cb: CallbackQuery):
    _, year, month = cb.data.split("|")
    uid  = cb.from_user.id
    y, m = int(year), int(month)
    text = build_dashboard(uid, y, m)
    kb   = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💸 Все расходы", callback_data=f"exp|{y}|{m}|0"),
            InlineKeyboardButton(text="🏆 Топ-10",      callback_data=f"top|{y}|{m}"),
        ],
        [InlineKeyboardButton(text="📄 PDF выписка",   callback_data=f"rpdf|{y}|{m}")],
    ])
    await cb.message.answer(text, reply_markup=kb)
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Все расходы списком
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith("exp|"))
async def cb_expenses(cb: CallbackQuery):
    parts  = cb.data.split("|")
    year   = int(parts[1])
    month  = int(parts[2])
    offset = int(parts[3])
    uid    = cb.from_user.id
    per    = 10
    text, total = build_expenses_list(uid, year, month, offset=offset, per=per)
    if total == 0:
        await cb.answer("Нет расходов за этот период", show_alert=True)
        return
    await cb.message.answer(text, reply_markup=kb_expenses_nav(year, month, offset, total, per))
    await cb.answer()

@dp.callback_query(F.data == "close_exp")
async def cb_close_exp(cb: CallbackQuery):
    await cb.message.delete()
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Сравнение месяцев
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data == "start_compare")
async def cb_start_compare(cb: CallbackQuery, state: FSMContext):
    await state.set_state(CompareForm.pick_month1)
    await cb.message.answer(
        "Выберите <b>первый</b> месяц для сравнения:",
        reply_markup=kb_month_picker("cmp1")
    )
    await cb.answer()

@dp.message(StateFilter("*"), F.text == "📈 Сравнить месяцы")
async def compare_months_btn(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await state.set_state(CompareForm.pick_month1)
    await msg.answer(
        "Выберите <b>первый</b> месяц:",
        reply_markup=kb_month_picker("cmp1")
    )

@dp.callback_query(F.data.startswith("cmp1|"))
async def cb_cmp1(cb: CallbackQuery, state: FSMContext):
    _, year, month = cb.data.split("|")
    await state.update_data(year1=int(year), month1=int(month))
    await state.set_state(CompareForm.pick_month2)
    mn = MONTH_NAMES.get(int(month), "")
    await cb.message.answer(
        f"Первый месяц: <b>{mn} {year}</b>\n\nТеперь выберите <b>второй</b> месяц:",
        reply_markup=kb_month_picker("cmp2")
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("cmp2|"))
async def cb_cmp2(cb: CallbackQuery, state: FSMContext):
    _, year, month = cb.data.split("|")
    data   = await state.get_data()
    uid    = cb.from_user.id
    y1, m1 = data.get("year1", datetime.now().year), data.get("month1", datetime.now().month)
    y2, m2 = int(year), int(month)
    await state.clear()
    text = build_compare(uid, y1, m1, y2, m2)
    await cb.message.answer(text)
    await cb.answer()

@dp.callback_query(F.data == "cancel_pick")
async def cb_cancel_pick(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    await cb.answer("Отменено")

@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Аналитика раздел
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "📈 Аналитика")
async def analytics_menu(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await msg.answer("Выберите тип аналитики:", reply_markup=kb_analytics())

@dp.message(StateFilter("*"), F.text == "📊 За этот месяц")
async def analytics_this_month(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    now  = datetime.now()
    text = build_dashboard(msg.from_user.id, now.year, now.month)
    await msg.answer(text)

@dp.message(StateFilter("*"), F.text == "📅 Выбрать месяц")
async def analytics_pick_month(msg: Message, state: FSMContext):
    await msg.answer("Выберите месяц:", reply_markup=kb_month_picker("dash"))

@dp.message(StateFilter("*"), F.text == "🏆 Топ-10 категорий")
async def analytics_top10(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    now  = datetime.now()
    cats = db.get_top_categories(msg.from_user.id, now.year, now.month, limit=10)
    if not cats:
        await msg.answer("Нет данных за этот месяц.", reply_markup=kb_analytics())
        return
    month_name = MONTH_NAMES.get(now.month, "")
    total = sum(float(c["total"]) for c in cats)
    text  = f"🏆 <b>Топ-10 расходов — {month_name} {now.year}</b>\n\n"
    max_v = float(cats[0]["total"])
    for i, c in enumerate(cats):
        v   = float(c["total"])
        pct = v / total * 100
        bar = _bar(v / max_v * 100, 10)
        text += f"{i+1:2}. {c['category'][:20]:<20}\n    {bar} {pct:.1f}%  <code>{v:,.0f} ₽</code>\n"
    await msg.answer(text)

@dp.message(StateFilter("*"), F.text == "💸 Все расходы")
async def analytics_all_expenses(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    now  = datetime.now()
    text, total = build_expenses_list(msg.from_user.id, now.year, now.month)
    if total == 0:
        await msg.answer("Нет расходов за этот месяц.", reply_markup=kb_analytics())
        return
    await msg.answer(
        text,
        reply_markup=kb_expenses_nav(now.year, now.month, 0, total)
    )

@dp.message(StateFilter("*"), F.text == "📄 Выписка PDF")
async def analytics_pdf(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    now = datetime.now()
    await msg.answer("⏳ Генерирую PDF выписку за текущий месяц...")
    try:
        pdf_bytes  = generate_pdf_report(msg.from_user.id, now.year, now.month)
        month_name = MONTH_NAMES.get(now.month, "")
        filename   = f"DizelFinance_{month_name}_{now.year}.pdf"
        await bot.send_document(
            msg.from_user.id,
            document=BufferedInputFile(pdf_bytes, filename=filename),
            caption=f"📄 Выписка за {month_name} {now.year}"
        )
    except ImportError:
        await msg.answer("❌ Нужно установить reportlab:\n<code>pip install reportlab</code>")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# Транзакции
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "📋 Транзакции")
async def my_txs(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    uid     = msg.from_user.id
    records = db.get_transactions(uid, limit=10)
    if not records:
        await msg.answer("📂 Транзакций пока нет.", reply_markup=kb_main())
        return
    text = "📋 <b>Последние 10 транзакций:</b>\n\n"
    for r in records:
        cur   = r.get("currency", "RUB")
        sym   = CURRENCY_SYMBOLS.get(cur, cur)
        amt   = float(r.get("amount", 0))
        a_rub = float(r.get("amount_rub", amt))
        icon  = "💰" if r.get("tx_type") == "Доход" else ("🏦" if r.get("tx_type") == "Актив" else "💸")
        d     = r.get("date", "")
        if hasattr(d, "strftime"): d = d.strftime("%d.%m.%Y")
        text += f"{icon} <b>{r.get('category','')}</b>"
        if r.get("merchant"):
            text += f" · <i>{r['merchant'][:15]}</i>"
        text += f"\n  📅 {d}  💰 {amt:,.0f} {sym}"
        if cur != "RUB": text += f" ({a_rub:,.0f} ₽)"
        text += f"\n{'─'*28}\n"
    await msg.answer(text, reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Черновики
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "📥 Черновики")
async def show_drafts(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    uid    = msg.from_user.id
    drafts = db.drafts_get(uid)
    if not drafts:
        await msg.answer("📭 Нет черновиков.", reply_markup=kb_main())
        return
    text = f"📥 <b>Черновики ({len(drafts)}):</b>\n\n"
    rows = []
    for i, d in enumerate(drafts):
        sym   = CURRENCY_SYMBOLS.get(d["cur"], d["cur"])
        icon  = "💰" if d.get("tx_type") == "Доход" else "💸"
        text += f"{i+1}. {icon} {d['a']:,.0f} {sym} — {d['m']} · {d['d']}\n"
        rows.append([InlineKeyboardButton(
            text=f"✏️ {d['m'][:20]} {d['a']:,.0f} {sym}",
            callback_data=f"dr|{d['id']}"
        )])
    rows.append([InlineKeyboardButton(text="🗑 Очистить все", callback_data="dr|clear")])
    await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data.startswith("dr|"))
async def cb_draft(cb: CallbackQuery, state: FSMContext):
    uid    = cb.from_user.id
    action = cb.data.split("|")[1]
    if action == "clear":
        db.drafts_clear(uid)
        await cb.message.edit_text("🗑 Черновики удалены.")
        await cb.answer()
        return
    drafts = db.drafts_get(uid)
    draft  = next((d for d in drafts if d["id"] == action), None)
    if not draft:
        await cb.message.edit_text("❌ Черновик не найден.")
        await cb.answer()
        return
    db.drafts_remove(action)
    tx_type = draft.get("tx_type", "Расход")
    await state.update_data(
        amount=draft["a"], currency=draft["cur"], rate=draft["rate"],
        amount_rub=draft["a_rub"], date=draft["d"],
        merchant=draft["m"], tx_type=tx_type,
    )
    sym = CURRENCY_SYMBOLS.get(draft["cur"], "₽")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer(
        f"✅ Продолжаем: <b>{draft['a']:,.0f} {sym}</b> — {draft['m']}\nВыберите раздел:",
        reply_markup=kb_section(tx_type)
    )
    await state.set_state(TxForm.section)
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Настройки
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "⚙️ Настройки")
async def settings(msg: Message):
    now = datetime.now()
    await msg.answer(
        f"⚙️ <b>DizelFinance v3</b>\n\n"
        f"👤 Ваш ID: <code>{msg.from_user.id}</code>\n"
        f"🗄 БД: PostgreSQL\n"
        f"📅 {MONTH_NAMES.get(now.month,'')} {now.year}\n\n"
        f"<b>Webhook endpoints:</b>\n"
        f"<code>POST /webhook/sms</code>\n"
        f"<code>POST /webhook/transaction</code>\n\n"
        f"<b>Категорий:</b> {len(ALL_CATEGORIES)}",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# FSM — Ручной ввод транзакции
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(TxForm.tx_type)
async def proc_tx_type(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await state.clear()
        await msg.answer("Главное меню:", reply_markup=kb_main())
        return
    MAP = {"💸 Расход": "Расход", "💰 Доход": "Доход", "🏦 Актив": "Актив"}
    tx_type = MAP.get(msg.text)
    if not tx_type:
        await msg.answer("Выберите тип:", reply_markup=kb_tx_type())
        return
    await state.update_data(tx_type=tx_type)
    await state.set_state(TxForm.section)
    await msg.answer("Выберите раздел:", reply_markup=kb_section(tx_type))

@dp.message(TxForm.section)
async def proc_section(msg: Message, state: FSMContext):
    data = await state.get_data()
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.tx_type)
        await msg.answer("Тип операции:", reply_markup=kb_tx_type())
        return
    section = SECTION_LABEL.get(msg.text)
    if not section:
        await msg.answer("Выберите раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))
        return
    await state.update_data(section=section)
    await state.set_state(TxForm.category)
    await msg.answer(f"Категория ({section}):", reply_markup=kb_categories(section))

@dp.message(TxForm.category)
async def proc_category(msg: Message, state: FSMContext):
    data    = await state.get_data()
    section = data.get("section", "")
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.section)
        await msg.answer("Раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))
        return
    valid = SECTIONS.get(section, {}).get("categories", [])
    if msg.text not in valid:
        await msg.answer("Выберите из списка:", reply_markup=kb_categories(section))
        return
    await state.update_data(category=msg.text)
    if data.get("from_pdf"):
        uid  = msg.from_user.id
        sess = pdf_sessions.get(uid)
        if sess:
            idx = data.get("pdf_idx", 0)
            sess["transactions"][idx]["category"] = msg.text
            sess["transactions"][idx]["section"]  = section
        updated = await state.get_data()
        await state.set_state(TxForm.confirm)
        await msg.answer(build_preview(updated), reply_markup=kb_confirm())
        return
    await state.set_state(TxForm.amount)
    await msg.answer("Введите сумму:", reply_markup=kb_back())

@dp.message(TxForm.amount)
async def proc_amount(msg: Message, state: FSMContext):
    data = await state.get_data()
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.category)
        await msg.answer("Категория:", reply_markup=kb_categories(data.get("section", "")))
        return
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        assert amount > 0
    except Exception:
        await msg.answer("Введите корректную сумму:", reply_markup=kb_back())
        return
    await state.update_data(amount=amount)
    await state.set_state(TxForm.currency)
    await msg.answer("Валюта:", reply_markup=kb_currencies())

@dp.message(TxForm.currency)
async def proc_currency(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.amount)
        await msg.answer("Сумма:", reply_markup=kb_back())
        return
    if msg.text not in CURRENCIES:
        await msg.answer("Выберите валюту:", reply_markup=kb_currencies())
        return
    data  = await state.get_data()
    rate  = get_rate(msg.text)
    a_rub = round(float(data.get("amount", 0)) * rate, 2)
    await state.update_data(currency=msg.text, rate=rate, amount_rub=a_rub)
    await state.set_state(TxForm.date)
    today = datetime.now().strftime("%d.%m.%Y, %H:%M")
    await msg.answer(
        "Дата и время:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text=today)],
            [KeyboardButton(text="⏪ Назад")],
        ], resize_keyboard=True, one_time_keyboard=True)
    )

@dp.message(TxForm.date)
async def proc_date(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.currency)
        await msg.answer("Валюта:", reply_markup=kb_currencies())
        return
    ok = False
    for fmt in ("%d.%m.%Y, %H:%M", "%d.%m.%Y,%H:%M", "%d.%m.%Y"):
        try:
            datetime.strptime(msg.text.strip(), fmt)
            ok = True
            break
        except ValueError:
            pass
    if not ok:
        await msg.answer("Формат: <code>09.03.2026, 14:35</code>", reply_markup=kb_back())
        return
    await state.update_data(date=msg.text.strip())
    data = await state.get_data()
    await state.set_state(TxForm.confirm)
    await msg.answer(build_preview(data), reply_markup=kb_confirm())

@dp.message(TxForm.confirm)
async def proc_confirm(msg: Message, state: FSMContext):
    data = await state.get_data()
    uid  = msg.from_user.id
    if msg.text == "✅ Записать":
        db.save_transaction(uid, {**data, "source": "manual"})
        _notify_admin(
            f"💾 Новая транзакция\n"
            f"📂 {data.get('section')} → {data.get('category')}\n"
            f"💰 {data.get('amount')} {data.get('currency')}"
        )
        if data.get("from_pdf"):
            idx  = data.get("pdf_idx", 0)
            sess = pdf_sessions.get(uid)
            if sess:
                sess["saved_count"] = sess.get("saved_count", 0) + 1
            await state.set_state(PDFReview.reviewing)
            await msg.answer("✅ Записано!", reply_markup=ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx + 1)
        else:
            await state.clear()
            await msg.answer("✅ Транзакция записана!", reply_markup=kb_main())
    elif msg.text == "✏️ Изменить категорию":
        await state.set_state(TxForm.section)
        await msg.answer("Раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))
    elif msg.text == "🔢 Изменить сумму":
        sym = CURRENCY_SYMBOLS.get(data.get("currency", "RUB"), "")
        await state.set_state(TxForm.edit_amount)
        await msg.answer(
            f"Текущая: <b>{data.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )
    elif msg.text == "❌ Отменить":
        if data.get("from_pdf"):
            idx = data.get("pdf_idx", 0)
            await state.set_state(PDFReview.reviewing)
            await msg.answer("Отменено.", reply_markup=ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx)
        else:
            await state.clear()
            await msg.answer("Отменено.", reply_markup=kb_main())

@dp.message(TxForm.edit_amount)
async def proc_edit_amount(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        data = await state.get_data()
        await state.set_state(TxForm.confirm)
        await msg.answer(build_preview(data), reply_markup=kb_confirm())
        return
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        assert amount > 0
    except Exception:
        await msg.answer("Введите корректную сумму:", reply_markup=kb_back())
        return
    await state.update_data(new_amount=amount)
    await state.set_state(TxForm.edit_currency)
    await msg.answer("Валюта:", reply_markup=kb_currencies())

@dp.message(TxForm.edit_currency)
async def proc_edit_currency(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.edit_amount)
        await msg.answer("Сумма:", reply_markup=kb_back())
        return
    if msg.text not in CURRENCIES:
        await msg.answer("Выберите валюту:", reply_markup=kb_currencies())
        return
    data       = await state.get_data()
    uid        = msg.from_user.id
    new_amount = float(data.get("new_amount", 0))
    rate       = get_rate(msg.text)
    a_rub      = round(new_amount * rate, 2)
    await state.update_data(amount=new_amount, currency=msg.text, rate=rate, amount_rub=a_rub)
    if data.get("from_pdf"):
        sess = pdf_sessions.get(uid)
        idx  = data.get("pdf_idx", 0)
        if sess and idx < len(sess["transactions"]):
            sess["transactions"][idx].update(amount=new_amount, currency=msg.text, rate=rate, amount_rub=a_rub)
    updated = await state.get_data()
    await state.set_state(TxForm.confirm)
    await msg.answer("✅ Обновлено!", reply_markup=ReplyKeyboardRemove())
    await msg.answer(build_preview(updated), reply_markup=kb_confirm())

# ══════════════════════════════════════════════════════════════════════════════
# Документы
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.document)
async def handle_document(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    fname = (msg.document.file_name or "").lower()
    if fname.endswith((".xlsx", ".xls")):
        await _handle_xlsx(msg)
    elif fname.endswith(".pdf"):
        await _handle_pdf(msg)
    elif fname.endswith('.txt'):
        await _handle_txt(msg)
    else:
        await msg.answer("Поддерживаются PDF, Excel и TXT файлы.")

async def _handle_xlsx(msg: Message):
    await msg.answer("⏳ Читаю Excel...")
    try:
        f   = await bot.get_file(msg.document.file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_xlsx(raw.read())
        if not txs:
            await msg.answer("❌ Транзакции не найдены.", reply_markup=kb_main())
            return
        await msg.answer(f"📊 Найдено <b>{len(txs)}</b> транзакций. Определяю категории...")
        enriched = _enrich(txs, msg.from_user.id)
        _store_session(msg.from_user.id, enriched)
        await _send_summary(msg, enriched, "Excel")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

async def _handle_pdf(msg: Message):
    await msg.answer("⏳ Читаю PDF через AI...")
    try:
        f   = await bot.get_file(msg.document.file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_pdf(raw.read())
        if not txs:
            await msg.answer("❌ Транзакции не найдены.", reply_markup=kb_main())
            return
        await msg.answer(f"📊 Найдено <b>{len(txs)}</b> транзакций. Определяю категории...")
        enriched = _enrich(txs, msg.from_user.id)
        _store_session(msg.from_user.id, enriched)
        await _send_summary(msg, enriched, "PDF")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

# вставить после _handle_pdf:
async def _handle_txt(msg: Message):
    await msg.answer("⏳ Читаю TXT файл через AI...")
    try:
        from txt_parser import parse_txt, read_txt_file
        f   = await bot.get_file(msg.document.file_id)
        raw = await bot.download_file(f.file_path)
        text = read_txt_file(raw.read())
        txs = parse_txt(text)
        if not txs:
            await msg.answer("❌ Транзакции не найдены.", reply_markup=kb_main())
            return
        await msg.answer(f"📊 Найдено <b>{len(txs)}</b> транзакций. Определяю категории...")
        enriched = _enrich(txs, msg.from_user.id)
        _store_session(msg.from_user.id, enriched)
        await _send_summary(msg, enriched, "TXT")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("pdf|"))
async def cb_pdf_action(cb: CallbackQuery, state: FSMContext):
    uid    = cb.from_user.id
    action = cb.data.split("|")[1]
    sess   = pdf_sessions.get(uid)
    if not sess:
        await cb.message.edit_text("❌ Сессия устарела.")
        await cb.answer()
        return
    if action == "cancel":
        pdf_sessions.pop(uid, None)
        await cb.message.edit_text("❌ Отменено.")
        await cb.message.answer("Главное меню:", reply_markup=kb_main())
    elif action == "all":
        await cb.message.edit_text("⏳ Записываю все...")
        saved = db.save_transactions_batch(uid, [{**tx, "source": "auto"} for tx in sess["transactions"]])
        pdf_sessions.pop(uid, None)
        await cb.message.answer(f"✅ Записано <b>{saved}</b> транзакций!", reply_markup=kb_main())
    elif action == "review":
        await state.set_state(PDFReview.reviewing)
        await cb.message.edit_text("👀 Просматриваем по одной...")
        await show_pdf_tx(cb.message, uid, 0)
    await cb.answer()

@dp.callback_query(F.data.startswith("pi|"))
async def cb_pdf_item(cb: CallbackQuery, state: FSMContext):
    uid   = cb.from_user.id
    parts = cb.data.split("|")
    act   = parts[1]
    idx   = int(parts[2])
    sess  = pdf_sessions.get(uid)
    if not sess:
        await cb.message.edit_text("❌ Сессия устарела.")
        await cb.answer()
        return
    txs = sess["transactions"]

    async def remove_kb():
        try: await cb.message.edit_reply_markup(reply_markup=None)
        except Exception: pass

    if act == "done":
        saved   = sess.get("saved_count", 0)
        skipped = sess.get("skipped_count", 0)
        pdf_sessions.pop(uid, None)
        await cb.message.edit_text(f"🏁 <b>Готово!</b>\n✅ {saved}\n⏭ {skipped}")
        await cb.message.answer("Главное меню:", reply_markup=kb_main())
        await state.clear()
    elif act == "save":
        db.save_transaction(uid, {**txs[idx], "source": "auto"})
        sess["saved_count"] += 1
        await cb.answer("✅ Записано!")
        await remove_kb()
        await show_pdf_tx(cb.message, uid, idx + 1)
    elif act == "skip":
        sess["skipped_count"] += 1
        await cb.answer("⏭ Пропущено")
        await remove_kb()
        await show_pdf_tx(cb.message, uid, idx + 1)
    elif act == "next":
        await remove_kb()
        await show_pdf_tx(cb.message, uid, idx + 1)
    elif act == "edit_cat":
        tx = txs[idx]
        await state.update_data(
            from_pdf=True, pdf_idx=idx,
            amount=float(tx.get("amount", 0)), currency=tx.get("currency", "RUB"),
            rate=float(tx.get("rate", 1.0)), amount_rub=float(tx.get("amount_rub", tx.get("amount", 0))),
            date=tx.get("date", ""), tx_type=tx.get("tx_type", "Расход"),
        )
        await remove_kb()
        await state.set_state(TxForm.section)
        await cb.message.answer(
            f"✏️ Категория для: <b>{tx.get('merchant','')}</b>",
            reply_markup=kb_section(tx.get("tx_type", "Расход"))
        )
    elif act == "edit_amt":
        tx  = txs[idx]
        sym = CURRENCY_SYMBOLS.get(tx.get("currency", "RUB"), "")
        await state.update_data(
            from_pdf=True, pdf_idx=idx,
            category=tx.get("category",""), section=tx.get("section",""),
            date=tx.get("date",""), tx_type=tx.get("tx_type","Расход"),
            currency=tx.get("currency","RUB"),
        )
        await remove_kb()
        await state.set_state(TxForm.edit_amount)
        await cb.message.answer(
            f"🔢 Текущая: <b>{tx.get('amount',0)} {sym}</b>\nНовая:",
            reply_markup=kb_back()
        )
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Фото
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.photo)
async def handle_photo(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await msg.answer("📸 Анализирую скриншот...")
    try:
        f   = await bot.get_file(msg.photo[-1].file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_screenshot(raw.read())
        if not txs:
            await msg.answer("❌ Транзакций не найдено.", reply_markup=kb_main())
            return
        if len(txs) == 1:
            await _send_single_tx(msg, txs[0])
        else:
            enriched = _enrich(txs, msg.from_user.id)
            _store_session(msg.from_user.id, enriched)
            await _send_summary(msg, enriched, "Скриншот")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Quick cat callbacks
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith("qc|"))
async def cb_quick_cat(cb: CallbackQuery, state: FSMContext):
    _, tx_id, cat, sec = cb.data.split("|", 3)
    tx = pending.pop(tx_id, None)
    if not tx:
        await cb.message.edit_text("❌ Транзакция устарела.")
        await cb.answer()
        return
    try: await cb.message.edit_reply_markup(reply_markup=None)
    except Exception: pass
    await state.update_data(
        amount=tx["a"], currency=tx["cur"], rate=tx["rate"],
        amount_rub=tx["a_rub"], date=tx["d"],
        tx_type=tx["tx_type"], category=cat, section=sec, merchant=tx["m"],
    )
    await state.set_state(TxForm.confirm)
    data = await state.get_data()
    await cb.message.answer(build_preview(data), reply_markup=kb_confirm())
    await cb.answer()

@dp.callback_query(F.data.startswith("qa|"))
async def cb_quick_all(cb: CallbackQuery, state: FSMContext):
    _, tx_id = cb.data.split("|", 1)
    tx = pending.get(tx_id)
    if not tx:
        await cb.message.edit_text("❌ Транзакция устарела.")
        await cb.answer()
        return
    await state.update_data(
        amount=tx["a"], currency=tx["cur"], rate=tx["rate"],
        amount_rub=tx["a_rub"], date=tx["d"],
        tx_type=tx["tx_type"], merchant=tx["m"],
    )
    try: await cb.message.edit_reply_markup(reply_markup=None)
    except Exception: pass
    await state.set_state(TxForm.section)
    await cb.message.answer("Выберите раздел:", reply_markup=kb_section(tx["tx_type"]))
    await cb.answer()

@dp.callback_query(F.data == "qn|skip")
async def cb_quick_skip(cb: CallbackQuery):
    try: await cb.message.edit_reply_markup(reply_markup=None)
    except Exception: pass
    await cb.message.answer("❌ Пропущено.", reply_markup=kb_main())
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# SMS и фоллбэк
# ══════════════════════════════════════════════════════════════════════════════

SMS_KEYWORDS = [
    "списано", "зачислено", "покупка", "оплата", "перевод",
    "баланс", "карта", "тенге", "рублей", "сом"
]

@dp.message(StateFilter("*"), F.text)
async def handle_text_fallback(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    text = msg.text or ""
    if len(text) > 20 and any(w in text.lower() for w in SMS_KEYWORDS):
        await msg.answer("📱 Похоже на SMS, разбираю...")
        tx = parse_sms(text)
        if tx:
            await _send_single_tx(msg, tx)
            return
    await msg.answer(
        "Используйте кнопки меню 👇\n\n"
        "Или отправьте скриншот / PDF / SMS текстом",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Flask webhook
# ══════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

def _send_tg_sync(chat_id: int, text: str, kb: list = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if kb:
        payload["reply_markup"] = {"inline_keyboard": kb}
    try:
        req.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
    except Exception as e:
        log.error(f"send_tg_sync: {e}")

@flask_app.route("/webhook/sms", methods=["POST"])
def webhook_sms():
    try:
        data     = request.json or {}
        user_id  = data.get("user_id")
        sms_text = data.get("sms", "").strip()
        if not user_id or not sms_text:
            return jsonify({"status": "error", "message": "Missing user_id or sms"}), 400
        if ALLOWED_IDS and int(user_id) not in ALLOWED_IDS:
            return jsonify({"status": "error", "message": "Unauthorized"}), 403
        tx = parse_sms(sms_text)
        if not tx:
            return jsonify({"status": "skip"}), 200
        amount   = float(tx.get("amount", 0))
        currency = tx.get("currency", "RUB")
        merchant = tx.get("merchant", "SMS")
        tx_type  = tx.get("tx_type", "Расход")
        date     = tx.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M")
        rate     = get_rate(currency)
        a_rub    = round(amount * rate, 2)
        sym      = CURRENCY_SYMBOLS.get(currency, currency)
        icon     = "💰" if tx_type == "Доход" else "💸"
        cat, sec = guess_category(merchant, amount, tx_type=tx_type)
        tx_id    = str(uuid.uuid4())[:8]
        uid_int  = int(user_id)
        pending[tx_id] = {
            "a": amount, "m": merchant, "d": date,
            "cur": currency, "rate": rate, "a_rub": a_rub,
            "tx_type": tx_type, "category": cat, "section": sec,
        }
        db.drafts_add(uid_int, {
            "id": str(uuid.uuid4())[:8], "a": amount, "m": merchant, "d": date,
            "cur": currency, "rate": rate, "a_rub": a_rub, "tx_type": tx_type,
        })
        sym_line = f"{amount:,.0f} ₽" if currency == "RUB" else f"{amount:,.2f} {sym}"
        text = (
            f"📱 <b>SMS транзакция</b>\n\n"
            f"{icon} {tx_type} | <code>{sym_line}</code>\n"
            f"🏪 {merchant}\n"
            f"📅 {date}\n\n"
            f"🤖 <b>{sec} → {cat}</b>"
        )
        kb = [
            [{"text": f"✅ {cat}", "callback_data": f"qc|{tx_id}|{cat}|{sec}"}],
            [
                {"text": "📋 Все категории", "callback_data": f"qa|{tx_id}"},
                {"text": "❌ Пропустить",    "callback_data": "qn|skip"},
            ],
        ]
        _send_tg_sync(uid_int, text, kb)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"webhook_sms: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@flask_app.route("/webhook/transaction", methods=["POST"])
def webhook_transaction():
    try:
        data    = request.json or {}
        user_id = data.get("user_id")
        if not user_id or (ALLOWED_IDS and int(user_id) not in ALLOWED_IDS):
            return jsonify({"status": "error", "message": "Unauthorized"}), 403
        amount   = float(data.get("amount", 0))
        currency = data.get("currency", "RUB")
        merchant = data.get("merchant", "Неизвестно")
        tx_type  = data.get("tx_type", "Расход")
        date     = data.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M")
        rate     = get_rate(currency)
        a_rub    = round(amount * rate, 2)
        sym      = CURRENCY_SYMBOLS.get(currency, currency)
        icon     = "💰" if tx_type == "Доход" else "💸"
        cat, sec = guess_category(merchant, amount, tx_type=tx_type)
        tx_id    = str(uuid.uuid4())[:8]
        uid_int  = int(user_id)
        pending[tx_id] = {
            "a": amount, "m": merchant, "d": date,
            "cur": currency, "rate": rate, "a_rub": a_rub,
            "tx_type": tx_type, "category": cat, "section": sec,
        }
        db.drafts_add(uid_int, {
            "id": str(uuid.uuid4())[:8], "a": amount, "m": merchant, "d": date,
            "cur": currency, "rate": rate, "a_rub": a_rub, "tx_type": tx_type,
        })
        sym_line = f"{amount:,.0f} ₽" if currency == "RUB" else f"{amount:,.2f} {sym}"
        rub_line = f"\n🔄 {a_rub:,.0f} ₽" if currency != "RUB" else ""
        text = (
            f"🔔 <b>Новая транзакция</b>\n\n"
            f"{icon} {tx_type} | <code>{sym_line}</code>{rub_line}\n"
            f"🏪 {merchant}\n"
            f"📅 {date}\n\n"
            f"🤖 <b>{sec} → {cat}</b>"
        )
        kb = [
            [{"text": f"✅ {cat}", "callback_data": f"qc|{tx_id}|{cat}|{sec}"}],
            [
                {"text": "📋 Все категории", "callback_data": f"qa|{tx_id}"},
                {"text": "❌ Пропустить",    "callback_data": "qn|skip"},
            ],
        ]
        _send_tg_sync(uid_int, text, kb)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"webhook_transaction: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": "3.0"}), 200

# ══════════════════════════════════════════════════════════════════════════════
# Запуск
# ══════════════════════════════════════════════════════════════════════════════

async def start_bot():
    log.info("🚀 DizelFinance Bot v3 запущен!")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(start_bot())