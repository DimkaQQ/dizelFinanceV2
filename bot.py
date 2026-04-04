# -*- coding: utf-8 -*-
"""
DizelFinance Bot v3 — aiogram 3, PostgreSQL, новые категории
"""

import logging
import uuid
import asyncio
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
)

import database as db
from ai import (
    guess_category, guess_categories_batch,
    parse_screenshot, parse_pdf, parse_xlsx, parse_sms,
)
from rates import get_rate
from config import (
    TELEGRAM_TOKEN, ALLOWED_IDS,
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
    waiting       = State()
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
        [KeyboardButton(text="➕ Новая транзакция")],
        [KeyboardButton(text="📋 Транзакции"), KeyboardButton(text="📊 Аналитика")],
        [KeyboardButton(text="📥 Отложенные"), KeyboardButton(text="⚙️ Настройки")],
    ], resize_keyboard=True)

def kb_tx_type() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💸 Расход"), KeyboardButton(text="💰 Доход")],
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
    rows = [[KeyboardButton(text=c)] for c in cats]
    rows.append([KeyboardButton(text="⏪ Назад")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=True)

def kb_currencies() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=c) for c in CURRENCIES],
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
            text=f"→ Следующая ({idx+2}/{total})",
            callback_data=f"pi|next|{idx}"
        )])
    else:
        rows.append([InlineKeyboardButton(text="🏁 Завершить", callback_data="pi|done|0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_quick_cats(tx_id: str, category: str, section: str) -> InlineKeyboardMarkup:
    cats = SECTIONS.get(section, {}).get("categories", [])
    alts = [c for c in cats if c != category][:3]
    rows = [
        [InlineKeyboardButton(
            text=f"🤖 {section} → {category}",
            callback_data=f"qc|{tx_id}|{category}|{section}"
        )]
    ]
    for alt in alts:
        rows.append([InlineKeyboardButton(text=alt, callback_data=f"qc|{tx_id}|{alt}|{section}")])
    rows.append([
        InlineKeyboardButton(text="📋 Все категории", callback_data=f"qa|{tx_id}"),
        InlineKeyboardButton(text="❌ Пропустить",    callback_data="qn|skip"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ══════════════════════════════════════════════════════════════════════════════
# Превью
# ══════════════════════════════════════════════════════════════════════════════

def build_preview(data: dict) -> str:
    cur        = data.get("currency", "RUB")
    amount     = float(data.get("amount", 0))
    rate       = float(data.get("rate", 1.0))
    amount_rub = float(data.get("amount_rub", amount))
    sym        = CURRENCY_SYMBOLS.get(cur, cur)
    tx_type    = data.get("tx_type", "Расход")
    icon       = "💰" if tx_type == "Доход" else ("🏦" if tx_type == "Актив" else "💸")

    text = (
        f"📝 <b>Предварительный просмотр</b>\n"
        f"{icon} <b>{tx_type}</b>\n"
        f"📅 Дата: <code>{data.get('date', '')}</code>\n"
        f"📂 Раздел: <code>{data.get('section', '')}</code>\n"
        f"📋 Категория: <code>{data.get('category', '')}</code>\n"
    )
    if cur == "RUB":
        text += f"💰 Сумма: <code>{amount:,.0f} ₽</code>\n"
    else:
        text += (
            f"💰 Сумма: <code>{amount:,.2f} {sym}</code>\n"
            f"💱 Курс: <code>{rate:,.4f} ₽/{sym}</code>\n"
            f"🔄 В рублях: <code>{amount_rub:,.0f} ₽</code>\n"
        )
    return text

def build_pdf_preview(tx: dict, idx: int, total: int) -> str:
    cur  = tx.get("currency", "RUB")
    sym  = CURRENCY_SYMBOLS.get(cur, cur)
    icon = "💰" if tx.get("tx_type") == "Доход" else "💸"
    text = (
        f"<b>#{idx+1} из {total}</b>\n"
        f"{icon} <b>{tx.get('merchant','')}</b>\n"
        f"💰 {float(tx.get('amount',0)):,.2f} {sym}\n"
        f"📅 {tx.get('date','')}\n"
        f"📂 {tx.get('section','')} → {tx.get('category','')}"
    )
    if tx.get("category_hint"):
        text += f"\n💡 Банк: <i>{tx['category_hint']}</i>"
    if tx.get("is_duplicate"):
        text += "\n⚠️ <i>Возможный дубликат</i>"
    return text

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
            "category":    cat,
            "section":     sec,
            "rate":        rate,
            "amount_rub":  a_rub,
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
        f"✅ <b>{label} обработан!</b>\n"
        f"📄 Транзакций: {len(enriched)}\n"
        f"🆕 Новых: {new_}\n"
        f"⚠️ Возможных дубликатов: {dup}\n"
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
    hint_line = f"\n💡 Банк: <i>{hint}</i>" if hint else ""
    await msg.answer(
        f"📸 <b>Распознано:</b>\n"
        f"{icon} {tx_type}\n"
        f"💵 {amount:,.2f} {sym}\n"
        f"🏪 {merchant}\n"
        f"📅 {date}{hint_line}\n"
        f"📂 <b>{sec}</b> → <b>{cat}</b>\n"
        f"Подтвердите или выберите другую категорию:",
        reply_markup=kb_quick_cats(tx_id, cat, sec)
    )

# ══════════════════════════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id):
        await msg.answer("🔒 Нет доступа.")
        return
    await state.set_state(TxForm.waiting)
    await msg.answer(
        "👋 <b>DizelFinance v3</b>\n\n"
        "📱 Способы добавить транзакцию:\n"
        "— <b>Скриншот</b> банка → AI распознает\n"
        "— <b>PDF выписка</b> → автопарсинг\n"
        "— <b>Excel выписка</b> → автопарсинг\n"
        "— <b>Текст SMS</b> → автораспознавание\n"
        "— <b>Ручной ввод</b> через меню\n\n"
        "Данные хранятся в PostgreSQL.",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Новая транзакция
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "➕ Новая транзакция")
async def new_tx(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(TxForm.tx_type)
    await msg.answer("Тип операции:", reply_markup=kb_tx_type())

@dp.message(TxForm.tx_type)
async def proc_tx_type(msg: Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await state.set_state(TxForm.waiting)
        await msg.answer("Главное меню:", reply_markup=kb_main())
        return
    MAP = {"💸 Расход": "Расход", "💰 Доход": "Доход", "🏦 Актив": "Актив"}
    tx_type = MAP.get(msg.text)
    if not tx_type:
        await msg.answer("Выберите тип:", reply_markup=kb_tx_type())
        return
    await state.update_data(tx_type=tx_type)
    await state.set_state(TxForm.section)
    await msg.answer("Раздел:", reply_markup=kb_section(tx_type))

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
        "Дата и время (или нажмите кнопку):",
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
        await msg.answer("Формат: ДД.ММ.ГГГГ, ЧЧ:ММ — например 09.03.2026, 14:35:", reply_markup=kb_back())
        return
    await state.update_data(date=msg.text.strip())
    data = await state.get_data()
    await state.set_state(TxForm.confirm)
    await msg.answer(build_preview(data), reply_markup=kb_confirm())

# ══════════════════════════════════════════════════════════════════════════════
# Подтверждение
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(TxForm.confirm)
async def proc_confirm(msg: Message, state: FSMContext):
    data = await state.get_data()
    uid  = msg.from_user.id

    if msg.text == "✅ Записать":
        db.save_transaction(uid, {**data, "source": "manual"})
        if data.get("from_pdf"):
            idx  = data.get("pdf_idx", 0)
            sess = pdf_sessions.get(uid)
            if sess:
                sess["saved_count"] = sess.get("saved_count", 0) + 1
            await state.set_state(PDFReview.reviewing)
            await msg.answer("✅ Записано!", reply_markup=ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx + 1)
        else:
            await state.set_state(TxForm.waiting)
            await msg.answer("✅ Транзакция записана!", reply_markup=kb_main())

    elif msg.text == "✏️ Изменить категорию":
        await state.set_state(TxForm.section)
        await msg.answer("Раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))

    elif msg.text == "🔢 Изменить сумму":
        sym = CURRENCY_SYMBOLS.get(data.get("currency", "RUB"), "")
        await state.set_state(TxForm.edit_amount)
        await msg.answer(
            f"Текущая сумма: <b>{data.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )

    elif msg.text == "❌ Отменить":
        if data.get("from_pdf"):
            idx = data.get("pdf_idx", 0)
            await state.set_state(PDFReview.reviewing)
            await msg.answer("Отменено.", reply_markup=ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx)
        else:
            await state.set_state(TxForm.waiting)
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

@dp.message(F.document)
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
        await msg.answer(f"📊 Найдено {len(txs)} транзакций. Определяю категории...")
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
        await msg.answer(f"📊 Найдено {len(txs)} транзакций. Определяю категории...")
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
        await cb.message.answer(f"✅ Записано {saved} транзакций!", reply_markup=kb_main())
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
        await state.set_state(TxForm.waiting)
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
            f"✏️ Редактирую категорию для:\n<b>{tx.get('merchant','')}</b>",
            reply_markup=kb_section(tx.get("tx_type", "Расход"))
        )
    elif act == "edit_amt":
        tx  = txs[idx]
        sym = CURRENCY_SYMBOLS.get(tx.get("currency", "RUB"), "")
        await state.update_data(
            from_pdf=True, pdf_idx=idx,
            category=tx.get("category", ""), section=tx.get("section", ""),
            date=tx.get("date", ""), tx_type=tx.get("tx_type", "Расход"),
            currency=tx.get("currency", "RUB"),
        )
        await remove_kb()
        await state.set_state(TxForm.edit_amount)
        await cb.message.answer(
            f"🔢 Текущая: <b>{tx.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Фото (скриншот)
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.photo)
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
        log.error(f"screenshot: {e}")
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
    await cb.message.answer(build_preview(await state.get_data()), reply_markup=kb_confirm())
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
# SMS текстом
# ══════════════════════════════════════════════════════════════════════════════

SMS_KEYWORDS = ["списано", "зачислено", "покупка", "оплата", "перевод",
                "баланс", "карта", "тенге", "рублей", "сом"]

@dp.message(TxForm.waiting, F.text)
async def handle_waiting(msg: Message, state: FSMContext):
    if not allowed(msg.from_user.id): return
    text = msg.text or ""
    if len(text) > 20 and any(w in text.lower() for w in SMS_KEYWORDS):
        await msg.answer("📱 Похоже на SMS, разбираю...")
        tx = parse_sms(text)
        if tx:
            await _send_single_tx(msg, tx)
            return
    await msg.answer("Используйте меню 👇", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Транзакции
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "📋 Транзакции")
async def my_txs(msg: Message):
    uid     = msg.from_user.id
    records = db.get_transactions(uid, limit=10)
    if not records:
        await msg.answer("📂 Нет транзакций.", reply_markup=kb_main())
        return
    text = "📋 <b>Последние 10 транзакций:</b>\n"
    for r in records:
        cur   = r.get("currency", "RUB")
        sym   = CURRENCY_SYMBOLS.get(cur, cur)
        amt   = float(r.get("amount", 0))
        a_rub = float(r.get("amount_rub", amt))
        icon  = "💰" if r.get("tx_type") == "Доход" else "💸"
        d     = r.get("date", "")
        if hasattr(d, "strftime"): d = d.strftime("%d.%m.%Y")
        text += f"{icon} <b>{r.get('category','')}</b>\n"
        text += f"  📅 {d} | {r.get('section','')}\n"
        text += f"  💰 {amt:,.0f} {sym}"
        if cur != "RUB": text += f" ({a_rub:,.0f} ₽)"
        text += f"\n{'─'*28}\n"
    await msg.answer(text, reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Аналитика
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "📊 Аналитика")
async def analytics(msg: Message):
    uid  = msg.from_user.id
    now  = datetime.now()
    data = db.get_monthly_summary(uid, now.year, now.month)
    if not data["sections"]:
        await msg.answer("📂 Нет данных за этот месяц.", reply_markup=kb_main())
        return
    month_name = MONTH_NAMES.get(now.month, "")
    text = f"📊 <b>Аналитика за {month_name} {now.year}:</b>\n\n"
    for row in data["sections"]:
        sec   = row.get("section", "")
        total = float(row.get("income") or 0) + float(row.get("expense") or 0) + float(row.get("assets") or 0)
        icon  = SECTIONS.get(sec, {}).get("icon", "📂")
        text += f"{icon} <b>{sec}</b>: {total:,.0f} ₽\n"
    text += (
        f"\n{'═'*28}\n"
        f"📈 Доходы:  {data['total_income']:,.0f} ₽\n"
        f"📉 Расходы: {data['total_expense']:,.0f} ₽\n"
        f"{'📊' if data['delta']>=0 else '⚠️'} Дельта: {data['delta']:+,.0f} ₽\n"
    )
    await msg.answer(text, reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Отложенные
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "📥 Отложенные")
async def show_drafts(msg: Message):
    uid    = msg.from_user.id
    drafts = db.drafts_get(uid)
    if not drafts:
        await msg.answer("📭 Нет отложенных транзакций.", reply_markup=kb_main())
        return
    text = f"📥 <b>Отложенные ({len(drafts)}):</b>\n"
    rows = []
    for i, d in enumerate(drafts):
        sym   = CURRENCY_SYMBOLS.get(d["cur"], d["cur"])
        text += f"{i+1}. {d['a']:,.0f} {sym} — {d['m']} ({d['d']})\n"
        rows.append([InlineKeyboardButton(
            text=f"✏️ #{i+1} {d['m']} {d['a']:,.0f} {sym}",
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
        f"✅ Продолжаем: {draft['a']:,.0f} {sym} — {draft['m']}\nВыберите раздел:",
        reply_markup=kb_section(tx_type)
    )
    await state.set_state(TxForm.section)
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Настройки
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(F.text == "⚙️ Настройки")
async def settings(msg: Message):
    now = datetime.now()
    await msg.answer(
        f"⚙️ <b>DizelFinance v3</b>\n"
        f"👤 Ваш ID: <code>{msg.from_user.id}</code>\n"
        f"🗄 БД: PostgreSQL\n"
        f"📅 Текущий месяц: {MONTH_NAMES.get(now.month,'')} {now.year}\n\n"
        f"<b>Разделы:</b>\n"
        f"💰 Доходы (5 категорий)\n"
        f"🛒 Регулярные расходы (20 категорий)\n"
        f"💳 Крупные траты (8 категорий)\n"
        f"🏦 Движение активов (5 категорий)",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Fallback
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter(None))
async def fallback(msg: Message):
    await msg.answer("Используйте меню 👇", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Запуск
# ══════════════════════════════════════════════════════════════════════════════

async def start_bot():
    log.info("🚀 DizelFinance Bot v3 (aiogram 3) запущен!")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(start_bot())