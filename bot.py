# -*- coding: utf-8 -*-
"""
DizelFinance Bot v3 — aiogram 3, PostgreSQL
"""

import logging
import uuid
import asyncio
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, default_state
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
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
# FSM состояния
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

# ══════════════════════════════════════════════════════════════════════════════
# Клавиатуры
# ══════════════════════════════════════════════════════════════════════════════

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить транзакцию")],
        [KeyboardButton(text="📋 Транзакции"), KeyboardButton(text="📊 Аналитика")],
        [KeyboardButton(text="📥 Черновики"),  KeyboardButton(text="⚙️ Настройки")],
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
    # по 2 кнопки в ряд для удобства
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
        text=f"🤖 {category} ✓",
        callback_data=f"qc|{tx_id}|{category}|{section}"
    )]]
    for alt in alts:
        rows.append([InlineKeyboardButton(text=alt, callback_data=f"qc|{tx_id}|{alt}|{section}")])
    rows.append([
        InlineKeyboardButton(text="📋 Все категории", callback_data=f"qa|{tx_id}"),
        InlineKeyboardButton(text="❌ Пропустить",    callback_data="qn|skip"),
    ])
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
    section    = data.get("section", "")
    category   = data.get("category", "")
    date       = data.get("date", "")
    merchant   = data.get("merchant", "")

    text = (
        f"╔══════════════════╗\n"
        f"  📝 Новая транзакция\n"
        f"╚══════════════════╝\n\n"
        f"{icon} <b>{tx_type}</b>\n"
        f"📅 <b>Дата:</b> {date}\n"
        f"📂 <b>Раздел:</b> {section}\n"
        f"🏷 <b>Категория:</b> {category}\n"
    )
    if merchant:
        text += f"🏪 <b>Место:</b> {merchant}\n"
    text += "\n"
    if cur == "RUB":
        text += f"💰 <b>Сумма:</b> <code>{amount:,.0f} ₽</code>\n"
    else:
        text += (
            f"💰 <b>Сумма:</b> <code>{amount:,.2f} {sym}</code>\n"
            f"💱 <b>Курс:</b> <code>{rate:,.4f} ₽/{sym}</code>\n"
            f"🔄 <b>В рублях:</b> <code>{amount_rub:,.0f} ₽</code>\n"
        )
    return text

def build_pdf_preview(tx: dict, idx: int, total: int) -> str:
    cur  = tx.get("currency", "RUB")
    sym  = CURRENCY_SYMBOLS.get(cur, cur)
    icon = "💰" if tx.get("tx_type") == "Доход" else "💸"
    text = (
        f"<b>Транзакция {idx+1} из {total}</b>\n\n"
        f"{icon} <b>{tx.get('merchant', '—')}</b>\n"
        f"💰 <code>{float(tx.get('amount',0)):,.2f} {sym}</code>\n"
        f"📅 {tx.get('date','—')}\n"
        f"📂 {tx.get('section','—')} → <b>{tx.get('category','—')}</b>"
    )
    if tx.get("category_hint"):
        text += f"\n💡 Банк: <i>{tx['category_hint']}</i>"
    if tx.get("is_duplicate"):
        text += "\n\n⚠️ <b>Возможный дубликат!</b>"
    return text

# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
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
        f"📄 Транзакций найдено: <b>{len(enriched)}</b>\n"
        f"🆕 Новых: <b>{new_}</b>\n"
        f"⚠️ Возможных дубликатов: <b>{dup}</b>\n\n"
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
            f"🏁 <b>Готово!</b>\n\n"
            f"✅ Записано: <b>{saved}</b>\n"
            f"⏭ Пропущено: <b>{skipped}</b>",
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
    hint_line = f"\n💡 Банк: <i>{hint}</i>" if hint else ""
    sym_line  = f"{amount:,.2f} {sym}" if cur != "RUB" else f"{amount:,.0f} ₽"
    await msg.answer(
        f"📸 <b>Распознано</b>\n\n"
        f"{icon} {tx_type} | <code>{sym_line}</code>\n"
        f"🏪 {merchant}\n"
        f"📅 {date}{hint_line}\n\n"
        f"🤖 Предлагаю: <b>{sec} → {cat}</b>\n"
        f"Подтвердите или выберите другую категорию:",
        reply_markup=kb_quick_cats(tx_id, cat, sec)
    )

def _notify_admin(text: str):
    if not ADMIN_ID:
        return
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
        "📸 Скриншот банка → AI распознает\n"
        "📄 PDF выписка → автопарсинг\n"
        "📊 Excel выписка → автопарсинг\n"
        "💬 Текст SMS → автораспознавание\n"
        "✏️ Кнопка «Добавить транзакцию»\n\n"
        "Выберите действие:",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Главное меню — кнопки работают из ЛЮБОГО состояния
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.text == "➕ Добавить транзакцию")
async def new_tx(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await state.clear()
    await state.set_state(TxForm.tx_type)
    await msg.answer(
        "Выберите тип операции:",
        reply_markup=kb_tx_type()
    )

@dp.message(StateFilter("*"), F.text == "📋 Транзакции")
async def my_txs(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    uid     = msg.from_user.id
    records = db.get_transactions(uid, limit=10)
    if not records:
        await msg.answer(
            "📂 Транзакций пока нет.\n\nДобавьте первую через «➕ Добавить транзакцию»",
            reply_markup=kb_main()
        )
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
            text += f" · {r['merchant']}"
        text += f"\n"
        text += f"   📅 {d}  |  {r.get('section','')}\n"
        text += f"   💰 {amt:,.0f} {sym}"
        if cur != "RUB": text += f"  ({a_rub:,.0f} ₽)"
        text += f"\n{'─' * 30}\n"
    await msg.answer(text, reply_markup=kb_main())

@dp.message(StateFilter("*"), F.text == "📊 Аналитика")
async def analytics(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    uid  = msg.from_user.id
    now  = datetime.now()
    data = db.get_monthly_summary(uid, now.year, now.month)
    month_name = MONTH_NAMES.get(now.month, "")

    if not data["sections"]:
        await msg.answer(
            f"📂 Нет данных за {month_name} {now.year}.",
            reply_markup=kb_main()
        )
        return

    text = f"📊 <b>Аналитика — {month_name} {now.year}</b>\n\n"
    for row in data["sections"]:
        sec   = row.get("section", "")
        total = (
            float(row.get("income")  or 0) +
            float(row.get("expense") or 0) +
            float(row.get("assets")  or 0)
        )
        icon  = SECTIONS.get(sec, {}).get("icon", "📂")
        color_total = f"{total:,.0f} ₽"
        text += f"{icon} <b>{sec}</b>\n"
        text += f"   {color_total} · {row.get('cnt',0)} транзакций\n\n"

    delta = data["delta"]
    delta_icon = "📈" if delta >= 0 else "📉"
    text += (
        f"{'═' * 30}\n"
        f"💚 Доходы:    <b>{data['total_income']:,.0f} ₽</b>\n"
        f"❤️ Расходы:  <b>{data['total_expense']:,.0f} ₽</b>\n"
        f"{delta_icon} Дельта:    <b>{delta:+,.0f} ₽</b>\n"
    )
    if data["total_assets"]:
        text += f"🏦 Активы:    <b>{data['total_assets']:,.0f} ₽</b>\n"

    # Кнопки для детализации
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Топ расходов",  callback_data=f"an|top|{now.year}|{now.month}"),
            InlineKeyboardButton(text="📅 За год",        callback_data=f"an|year|{now.year}|0"),
        ],
        [InlineKeyboardButton(text="📈 Тренд по месяцам", callback_data=f"an|trend|{now.year}|0")],
    ])
    await msg.answer(text, reply_markup=kb_main())
    await msg.answer("Детализация:", reply_markup=kb)

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

@dp.message(StateFilter("*"), F.text == "⚙️ Настройки")
async def settings(msg: Message, state: FSMContext):
    now = datetime.now()
    await msg.answer(
        f"⚙️ <b>DizelFinance v3</b>\n\n"
        f"👤 Ваш ID: <code>{msg.from_user.id}</code>\n"
        f"🗄 База данных: PostgreSQL\n"
        f"📅 Текущий месяц: {MONTH_NAMES.get(now.month,'')} {now.year}\n\n"
        f"<b>Категории:</b>\n"
        f"💰 Доходы — 5 категорий\n"
        f"🛒 Регулярные расходы — 20 категорий\n"
        f"💳 Крупные траты — 8 категорий\n"
        f"🏦 Движение активов — 5 категорий\n\n"
        f"<b>Webhook для iPhone Shortcut:</b>\n"
        f"<code>POST /webhook/sms</code>\n"
        f"<code>POST /webhook/transaction</code>",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Аналитика — inline callbacks
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query(F.data.startswith("an|"))
async def cb_analytics(cb: CallbackQuery):
    parts  = cb.data.split("|")
    action = parts[1]
    year   = int(parts[2])
    month  = int(parts[3])
    uid    = cb.from_user.id

    if action == "top":
        cats = db.get_top_categories(uid, year, month, limit=8)
        if not cats:
            await cb.answer("Нет данных", show_alert=True)
            return
        month_name = MONTH_NAMES.get(month, "")
        text = f"🏆 <b>Топ расходов — {month_name} {year}</b>\n\n"
        max_val = float(cats[0]["total"]) if cats else 1
        for i, c in enumerate(cats):
            total = float(c["total"])
            pct   = total / max_val * 100
            bar   = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
            text += f"{i+1}. <b>{c['category']}</b>\n"
            text += f"   {bar} {total:,.0f} ₽\n"
        await cb.message.answer(text)

    elif action == "year":
        data = db.get_monthly_summary_year(uid, year)
        text = f"📅 <b>Итоги {year} года по месяцам:</b>\n\n"
        for row in data:
            m_name = MONTH_NAMES.get(row["month"], "")[:3]
            income  = float(row.get("income")  or 0)
            expense = float(row.get("expense") or 0)
            delta   = income - expense
            d_icon  = "📈" if delta >= 0 else "📉"
            text += (
                f"<b>{m_name}</b>: "
                f"💚{income:,.0f} ❤️{expense:,.0f} {d_icon}{delta:+,.0f}\n"
            )
        await cb.message.answer(text)

    elif action == "trend":
        trend = db.get_yearly_trend(uid, years=3)
        text  = f"📈 <b>Тренд за последние 3 года:</b>\n\n"
        cur_year = None
        for row in trend:
            y = int(row["year"])
            m = int(row["month"])
            if y != cur_year:
                cur_year = y
                text += f"\n<b>{y}:</b>\n"
            m_name  = MONTH_NAMES.get(m, "")[:3]
            income  = float(row.get("income")  or 0)
            expense = float(row.get("expense") or 0)
            text += f"  {m_name}: 💚{income:,.0f} ❤️{expense:,.0f}\n"
        await cb.message.answer(text)

    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# FSM — Новая транзакция
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
        await msg.answer("Выберите тип операции:", reply_markup=kb_tx_type())
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
    await msg.answer(
        f"Выберите категорию\n<i>{section}</i>:",
        reply_markup=kb_categories(section)
    )

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
        await msg.answer("Выберите категорию из списка:", reply_markup=kb_categories(section))
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
        await msg.answer("Введите корректную сумму (например: 5000 или 5000.50):", reply_markup=kb_back())
        return
    await state.update_data(amount=amount)
    await state.set_state(TxForm.currency)
    await msg.answer("Выберите валюту:", reply_markup=kb_currencies())

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
        "Введите дату или нажмите кнопку для текущей:",
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
        await msg.answer(
            "Формат: <code>ДД.ММ.ГГГГ, ЧЧ:ММ</code>\nНапример: <code>09.03.2026, 14:35</code>",
            reply_markup=kb_back()
        )
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
            f"👤 ID: {uid}\n"
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
            f"Текущая сумма: <b>{data.get('amount',0)} {sym}</b>\nВведите новую:",
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
    await msg.answer("Выберите валюту:", reply_markup=kb_currencies())

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
            sess["transactions"][idx].update(
                amount=new_amount, currency=msg.text, rate=rate, amount_rub=a_rub
            )
    updated = await state.get_data()
    await state.set_state(TxForm.confirm)
    await msg.answer("✅ Сумма обновлена!", reply_markup=ReplyKeyboardRemove())
    await msg.answer(build_preview(updated), reply_markup=kb_confirm())

# ══════════════════════════════════════════════════════════════════════════════
# Документы (PDF / XLSX)
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.document)
async def handle_document(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    fname = (msg.document.file_name or "").lower()
    if fname.endswith((".xlsx", ".xls")):
        await _handle_xlsx(msg)
    elif fname.endswith(".pdf"):
        await _handle_pdf(msg)
    else:
        await msg.answer("Поддерживаются PDF и Excel файлы.")

async def _handle_xlsx(msg: Message):
    await msg.answer("⏳ Читаю Excel выписку...")
    try:
        f   = await bot.get_file(msg.document.file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_xlsx(raw.read())
        if not txs:
            await msg.answer("❌ Транзакции не найдены.", reply_markup=kb_main())
            return
        await msg.answer(f"📊 Найдено <b>{len(txs)}</b> транзакций. Определяю категории через AI...")
        enriched = _enrich(txs, msg.from_user.id)
        _store_session(msg.from_user.id, enriched)
        await _send_summary(msg, enriched, "Excel")
    except Exception as e:
        log.error(f"xlsx: {e}")
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

async def _handle_pdf(msg: Message):
    await msg.answer("⏳ Читаю PDF выписку через AI...")
    try:
        f   = await bot.get_file(msg.document.file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_pdf(raw.read())
        if not txs:
            await msg.answer("❌ Транзакции не найдены.", reply_markup=kb_main())
            return
        await msg.answer(f"📊 Найдено <b>{len(txs)}</b> транзакций. Определяю категории через AI...")
        enriched = _enrich(txs, msg.from_user.id)
        _store_session(msg.from_user.id, enriched)
        await _send_summary(msg, enriched, "PDF")
    except Exception as e:
        log.error(f"pdf: {e}")
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# PDF callbacks
# ══════════════════════════════════════════════════════════════════════════════

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
        await cb.message.edit_text("⏳ Записываю все транзакции...")
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
        await cb.message.edit_text(
            f"🏁 <b>Готово!</b>\n✅ Записано: {saved}\n⏭ Пропущено: {skipped}"
        )
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
            amount=float(tx.get("amount", 0)),
            currency=tx.get("currency", "RUB"),
            rate=float(tx.get("rate", 1.0)),
            amount_rub=float(tx.get("amount_rub", tx.get("amount", 0))),
            date=tx.get("date", ""),
            tx_type=tx.get("tx_type", "Расход"),
        )
        await remove_kb()
        await state.set_state(TxForm.section)
        await cb.message.answer(
            f"✏️ Редактирую категорию\n<b>{tx.get('merchant','')}</b>",
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
            f"🔢 Текущая: <b>{tx.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Фото — скриншот банка
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.photo)
async def handle_photo(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    await msg.answer("📸 Анализирую скриншот через AI...")
    try:
        f   = await bot.get_file(msg.photo[-1].file_id)
        raw = await bot.download_file(f.file_path)
        txs = parse_screenshot(raw.read())
        if not txs:
            await msg.answer(
                "❌ Транзакций не найдено.\n\n"
                "Попробуйте:\n"
                "— Скриншот списка операций\n"
                "— PDF выписку из банка",
                reply_markup=kb_main()
            )
            return
        if len(txs) == 1:
            await _send_single_tx(msg, txs[0])
        else:
            enriched = _enrich(txs, msg.from_user.id)
            _store_session(msg.from_user.id, enriched)
            await _send_summary(msg, enriched, "Скриншот")
    except Exception as e:
        log.error(f"screenshot: {e}")
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Quick cat inline callbacks
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
# Черновики callbacks
# ══════════════════════════════════════════════════════════════════════════════

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
# SMS текстом
# ══════════════════════════════════════════════════════════════════════════════

SMS_KEYWORDS = [
    "списано", "зачислено", "покупка", "оплата", "перевод",
    "баланс", "карта", "тенге", "рублей", "сом", "withdrawal", "payment"
]

@dp.message(StateFilter("*"), F.text)
async def handle_text_fallback(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    text = msg.text or ""
    # Проверяем на SMS
    if len(text) > 20 and any(w in text.lower() for w in SMS_KEYWORDS):
        await msg.answer("📱 Похоже на банковское SMS, разбираю...")
        tx = parse_sms(text)
        if tx:
            await _send_single_tx(msg, tx)
            return
    # Фоллбэк
    await msg.answer(
        "Используйте кнопки меню 👇\n\n"
        "Или отправьте:\n"
        "📸 Скриншот банка\n"
        "📄 PDF / Excel выписку\n"
        "💬 Текст SMS",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Flask webhook для iPhone Shortcut / n8n
# ══════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

def _send_tg_sync(chat_id: int, text: str, kb=None):
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
            return jsonify({"status": "skip", "message": "Not a transaction SMS"}), 200

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

        sym_line = f"{amount:,.2f} {sym}" if currency != "RUB" else f"{amount:,.0f} ₽"
        text = (
            f"📱 <b>SMS транзакция</b>\n\n"
            f"{icon} {tx_type} | <code>{sym_line}</code>\n"
            f"🏪 {merchant}\n"
            f"📅 {date}\n\n"
            f"🤖 Категория: <b>{sec} → {cat}</b>"
        )
        kb = [
            [{"text": f"✅ {cat}", "callback_data": f"qc|{tx_id}|{cat}|{sec}"}],
            [
                {"text": "📋 Все категории", "callback_data": f"qa|{tx_id}"},
                {"text": "❌ Пропустить",    "callback_data": "qn|skip"},
            ]
        ]
        _send_tg_sync(uid_int, text, kb)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        log.error(f"webhook_sms: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@flask_app.route("/webhook/transaction", methods=["POST"])
def webhook_transaction():
    try:
        data     = request.json or {}
        user_id  = data.get("user_id")
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

        sym_line = f"{amount:,.2f} {sym}" if currency != "RUB" else f"{amount:,.0f} ₽"
        rub_line = f"\n🔄 {a_rub:,.0f} ₽" if currency != "RUB" else ""
        text = (
            f"🔔 <b>Новая транзакция</b>\n\n"
            f"{icon} {tx_type} | <code>{sym_line}</code>{rub_line}\n"
            f"🏪 {merchant}\n"
            f"📅 {date}\n\n"
            f"🤖 Категория: <b>{sec} → {cat}</b>"
        )
        kb = [
            [{"text": f"✅ {cat}", "callback_data": f"qc|{tx_id}|{cat}|{sec}"}],
            [
                {"text": "📋 Все категории", "callback_data": f"qa|{tx_id}"},
                {"text": "❌ Пропустить",    "callback_data": "qn|skip"},
            ]
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
    log.info("🚀 DizelFinance Bot v3 (aiogram 3) запущен!")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(start_bot())