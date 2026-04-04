# -*- coding: utf-8 -*-
"""
DizelFinance Bot v3 — aiogram 2, PostgreSQL, новые категории
"""

import logging
import uuid
import threading
from datetime import datetime

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

import database as db
from ai import (
    guess_category, guess_categories_batch,
    parse_screenshot, parse_pdf, parse_xlsx, parse_sms,
)
from rates import get_rate
from config import (
    TELEGRAM_TOKEN, ALLOWED_IDS, ADMIN_ID,
    CURRENCIES, CURRENCY_SYMBOLS,
    INCOME_CATS, EXPENSE_CATS, ASSET_CATS,
    SECTIONS, ALL_CATEGORIES, MONTH_NAMES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

db.init_db()

bot     = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp      = Dispatcher(bot, storage=storage)

# Хранилище pending (webhook / screenshot)
pending: dict = {}

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

def kb_main():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("➕ Новая транзакция")
    kb.row("📋 Транзакции", "📊 Аналитика")
    kb.row("📥 Отложенные", "⚙️ Настройки")
    return kb

def kb_tx_type():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row("💸 Расход", "💰 Доход")
    kb.row("🏦 Актив")
    kb.row("⏪ Назад")
    return kb

def kb_section(tx_type: str):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    if tx_type == "Доход":
        kb.add("💰 Доходы")
    elif tx_type == "Актив":
        kb.add("🏦 Движение активов")
    else:
        kb.row("🛒 Регулярные расходы")
        kb.row("💳 Крупные траты")
    kb.add("⏪ Назад")
    return kb

SECTION_LABEL = {
    "💰 Доходы":           "Доходы",
    "🏦 Движение активов": "Движение активов",
    "🛒 Регулярные расходы": "Регулярные расходы",
    "💳 Крупные траты":    "Крупные траты",
}

def kb_categories(section: str):
    cats = SECTIONS.get(section, {}).get("categories", [])
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    for cat in cats:
        kb.add(cat)
    kb.add("⏪ Назад")
    return kb

def kb_currencies():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(*CURRENCIES)
    kb.add("⏪ Назад")
    return kb

def kb_back():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add("⏪ Назад")
    return kb

def kb_confirm():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.row("✅ Записать")
    kb.row("✏️ Изменить категорию", "🔢 Изменить сумму")
    kb.row("❌ Отменить")
    return kb

def kb_pdf_action():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Записать все", callback_data="pdf|all"),
        types.InlineKeyboardButton("👀 По одной",    callback_data="pdf|review"),
    )
    kb.add(types.InlineKeyboardButton("❌ Отменить", callback_data="pdf|cancel"))
    return kb

def kb_pdf_item(idx: int, total: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Записать",   callback_data=f"pi|save|{idx}"),
        types.InlineKeyboardButton("⏭ Пропустить", callback_data=f"pi|skip|{idx}"),
    )
    kb.add(
        types.InlineKeyboardButton("✏️ Категория",  callback_data=f"pi|edit_cat|{idx}"),
        types.InlineKeyboardButton("🔢 Сумма",      callback_data=f"pi|edit_amt|{idx}"),
    )
    if idx + 1 < total:
        kb.add(types.InlineKeyboardButton(
            f"→ Следующая ({idx+2}/{total})", callback_data=f"pi|next|{idx}"
        ))
    else:
        kb.add(types.InlineKeyboardButton("🏁 Завершить", callback_data="pi|done|0"))
    return kb

def kb_quick_cats(tx_id: str, category: str, section: str, tx_type: str):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(
        f"🤖 {section} → {category}",
        callback_data=f"qc|{tx_id}|{category}|{section}"
    ))
    cats = SECTIONS.get(section, {}).get("categories", [])
    alts = [c for c in cats if c != category][:3]
    for alt in alts:
        kb.add(types.InlineKeyboardButton(alt, callback_data=f"qc|{tx_id}|{alt}|{section}"))
    kb.add(
        types.InlineKeyboardButton("📋 Все категории", callback_data=f"qa|{tx_id}"),
        types.InlineKeyboardButton("❌ Пропустить",    callback_data="qn|skip"),
    )
    return kb

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

    text = (
        f"📝 <b>Предварительный просмотр</b>\n"
        f"{icon} <b>{tx_type}</b>\n"
        f"📅 Дата: <code>{date}</code>\n"
        f"📂 Раздел: <code>{section}</code>\n"
        f"📋 Категория: <code>{category}</code>\n"
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
    cur    = tx.get("currency", "RUB")
    sym    = CURRENCY_SYMBOLS.get(cur, cur)
    icon   = "💰" if tx.get("tx_type") == "Доход" else "💸"
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
# Сохранение в БД
# ══════════════════════════════════════════════════════════════════════════════

def save_tx(user_id: int, data: dict, source: str = "manual"):
    data["source"] = source
    db.save_transaction(user_id, data)
    log.info(f"✅ Saved: {data.get('category')} {data.get('amount')} {data.get('currency')}")

# ══════════════════════════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(commands=["start"], state="*")
async def cmd_start(msg: types.Message, state: FSMContext):
    if ALLOWED_IDS and msg.from_user.id not in ALLOWED_IDS:
        await msg.answer("🔒 Нет доступа.")
        return
    await state.finish()
    await TxForm.waiting.set()
    await msg.answer(
        "👋 <b>DizelFinance v3</b>\n\n"
        "📱 Способы добавить транзакцию:\n"
        "— <b>Скриншот</b> банка → AI распознает\n"
        "— <b>PDF выписка</b> → автопарсинг\n"
        "— <b>Excel выписка</b> → автопарсинг\n"
        "— <b>Текст SMS</b> → автораспознавание\n"
        "— <b>Ручной ввод</b> через меню\n\n"
        "Данные хранятся в PostgreSQL.\n"
        "Web-интерфейс: /web",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Новая транзакция — ручной ввод
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "➕ Новая транзакция", state="*")
async def new_tx(msg: types.Message, state: FSMContext):
    await state.finish()
    await TxForm.tx_type.set()
    await msg.answer("Тип операции:", reply_markup=kb_tx_type())

@dp.message_handler(state=TxForm.tx_type)
async def proc_tx_type(msg: types.Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await TxForm.waiting.set()
        await msg.answer("Главное меню:", reply_markup=kb_main())
        return
    MAP = {"💸 Расход": "Расход", "💰 Доход": "Доход", "🏦 Актив": "Актив"}
    tx_type = MAP.get(msg.text)
    if not tx_type:
        await msg.answer("Выберите тип:", reply_markup=kb_tx_type())
        return
    await state.update_data(tx_type=tx_type)
    await TxForm.section.set()
    await msg.answer("Раздел:", reply_markup=kb_section(tx_type))

@dp.message_handler(state=TxForm.section)
async def proc_section(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    if msg.text == "⏪ Назад":
        await TxForm.tx_type.set()
        await msg.answer("Тип операции:", reply_markup=kb_tx_type())
        return
    section = SECTION_LABEL.get(msg.text)
    if not section:
        await msg.answer("Выберите раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))
        return
    await state.update_data(section=section)
    await TxForm.category.set()
    await msg.answer(f"Категория ({section}):", reply_markup=kb_categories(section))

@dp.message_handler(state=TxForm.category)
async def proc_category(msg: types.Message, state: FSMContext):
    data    = await state.get_data()
    section = data.get("section", "")
    if msg.text == "⏪ Назад":
        await TxForm.section.set()
        await msg.answer("Раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))
        return
    valid = SECTIONS.get(section, {}).get("categories", [])
    if msg.text not in valid:
        await msg.answer("Выберите категорию из списка:", reply_markup=kb_categories(section))
        return
    await state.update_data(category=msg.text)
    if data.get("from_pdf"):
        # редактирование категории в PDF-сессии
        uid = msg.from_user.id
        sess = pdf_sessions.get(uid)
        if sess:
            idx = data.get("pdf_idx", 0)
            sess["transactions"][idx]["category"] = msg.text
            sess["transactions"][idx]["section"]  = section
        updated = await state.get_data()
        await TxForm.confirm.set()
        await msg.answer(build_preview(updated), reply_markup=kb_confirm())
        return
    await TxForm.amount.set()
    await msg.answer("Введите сумму:", reply_markup=kb_back())

@dp.message_handler(state=TxForm.amount)
async def proc_amount(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    if msg.text == "⏪ Назад":
        await TxForm.category.set()
        await msg.answer("Категория:", reply_markup=kb_categories(data.get("section", "")))
        return
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        assert amount > 0
    except Exception:
        await msg.answer("Введите корректную сумму:", reply_markup=kb_back())
        return
    await state.update_data(amount=amount)
    await TxForm.currency.set()
    await msg.answer("Валюта:", reply_markup=kb_currencies())

@dp.message_handler(state=TxForm.currency)
async def proc_currency(msg: types.Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await TxForm.amount.set()
        await msg.answer("Сумма:", reply_markup=kb_back())
        return
    if msg.text not in CURRENCIES:
        await msg.answer("Выберите валюту:", reply_markup=kb_currencies())
        return
    data  = await state.get_data()
    rate  = get_rate(msg.text)
    a_rub = round(float(data.get("amount", 0)) * rate, 2)
    await state.update_data(currency=msg.text, rate=rate, amount_rub=a_rub)
    await TxForm.date.set()
    today = datetime.now().strftime("%d.%m.%Y, %H:%M")
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(today)
    kb.add("⏪ Назад")
    await msg.answer("Дата и время (или нажмите кнопку):", reply_markup=kb)

@dp.message_handler(state=TxForm.date)
async def proc_date(msg: types.Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await TxForm.currency.set()
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
    await TxForm.confirm.set()
    await msg.answer(build_preview(data), reply_markup=kb_confirm())

# ══════════════════════════════════════════════════════════════════════════════
# Подтверждение
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(state=TxForm.confirm)
async def proc_confirm(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    uid  = msg.from_user.id

    if msg.text == "✅ Записать":
        save_tx(uid, data)
        if data.get("from_pdf"):
            idx = data.get("pdf_idx", 0)
            sess = pdf_sessions.get(uid)
            if sess:
                sess["saved_count"] = sess.get("saved_count", 0) + 1
            await state.finish()
            await PDFReview.reviewing.set()
            await msg.answer("✅ Записано!", reply_markup=types.ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx + 1)
        else:
            await state.finish()
            await TxForm.waiting.set()
            await msg.answer("✅ Транзакция записана!", reply_markup=kb_main())

    elif msg.text == "✏️ Изменить категорию":
        await TxForm.section.set()
        await msg.answer("Раздел:", reply_markup=kb_section(data.get("tx_type", "Расход")))

    elif msg.text == "🔢 Изменить сумму":
        sym = CURRENCY_SYMBOLS.get(data.get("currency", "RUB"), "")
        await TxForm.edit_amount.set()
        await msg.answer(
            f"Текущая сумма: <b>{data.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )

    elif msg.text == "❌ Отменить":
        if data.get("from_pdf"):
            idx = data.get("pdf_idx", 0)
            await state.finish()
            await PDFReview.reviewing.set()
            await msg.answer("Отменено.", reply_markup=types.ReplyKeyboardRemove())
            await show_pdf_tx(msg, uid, idx)
        else:
            await state.finish()
            await TxForm.waiting.set()
            await msg.answer("Отменено.", reply_markup=kb_main())

@dp.message_handler(state=TxForm.edit_amount)
async def proc_edit_amount(msg: types.Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        data = await state.get_data()
        await TxForm.confirm.set()
        await msg.answer(build_preview(data), reply_markup=kb_confirm())
        return
    try:
        amount = float(msg.text.replace(",", ".").replace(" ", ""))
        assert amount > 0
    except Exception:
        await msg.answer("Введите корректную сумму:", reply_markup=kb_back())
        return
    await state.update_data(new_amount=amount)
    await TxForm.edit_currency.set()
    await msg.answer("Валюта:", reply_markup=kb_currencies())

@dp.message_handler(state=TxForm.edit_currency)
async def proc_edit_currency(msg: types.Message, state: FSMContext):
    if msg.text == "⏪ Назад":
        await TxForm.edit_amount.set()
        await msg.answer("Сумма:", reply_markup=kb_back())
        return
    if msg.text not in CURRENCIES:
        await msg.answer("Выберите валюту:", reply_markup=kb_currencies())
        return
    data      = await state.get_data()
    uid       = msg.from_user.id
    new_amount = float(data.get("new_amount", 0))
    rate       = get_rate(msg.text)
    a_rub      = round(new_amount * rate, 2)
    await state.update_data(amount=new_amount, currency=msg.text, rate=rate, amount_rub=a_rub)

    # если редактируем в PDF-сессии — обновляем и там
    if data.get("from_pdf"):
        sess = pdf_sessions.get(uid)
        idx  = data.get("pdf_idx", 0)
        if sess and idx < len(sess["transactions"]):
            sess["transactions"][idx].update(
                amount=new_amount, currency=msg.text, rate=rate, amount_rub=a_rub
            )

    updated = await state.get_data()
    await TxForm.confirm.set()
    await msg.answer("✅ Сумма обновлена!", reply_markup=types.ReplyKeyboardRemove())
    await msg.answer(build_preview(updated), reply_markup=kb_confirm())

# ══════════════════════════════════════════════════════════════════════════════
# PDF / XLSX / Screenshot — сессии
# ══════════════════════════════════════════════════════════════════════════════

pdf_sessions: dict = {}

def _enrich(transactions: list[dict], uid: int) -> list[dict]:
    existing  = db.get_existing_keys(uid)
    cat_results = guess_categories_batch(transactions)
    enriched = []
    for tx, (cat, sec) in zip(transactions, cat_results):
        cur  = tx.get("currency", "RUB")
        rate = get_rate(cur)
        a    = float(tx.get("amount", 0))
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

def _store_session(uid: int, enriched: list[dict]):
    pdf_sessions[uid] = {
        "transactions":  enriched,
        "saved_count":   0,
        "skipped_count": 0,
    }

async def _send_summary(msg: types.Message, enriched: list[dict], label: str):
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

@dp.message_handler(content_types=types.ContentType.DOCUMENT, state="*")
async def handle_document(msg: types.Message, state: FSMContext):
    if ALLOWED_IDS and msg.from_user.id not in ALLOWED_IDS: return
    fname = (msg.document.file_name or "").lower()
    if fname.endswith((".xlsx", ".xls")):
        await _handle_xlsx(msg)
    elif fname.endswith(".pdf"):
        await _handle_pdf(msg)
    else:
        await msg.answer("Поддерживаются PDF и Excel файлы.")

async def _handle_xlsx(msg: types.Message):
    await msg.answer("⏳ Читаю Excel выписку...")
    try:
        f = await bot.get_file(msg.document.file_id)
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

async def _handle_pdf(msg: types.Message):
    await msg.answer("⏳ Читаю PDF выписку через AI...")
    try:
        f = await bot.get_file(msg.document.file_id)
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

@dp.callback_query_handler(lambda c: c.data.startswith("pdf|"), state="*")
async def cb_pdf_action(cb: types.CallbackQuery, state: FSMContext):
    uid    = cb.from_user.id
    action = cb.data.split("|")[1]
    sess   = pdf_sessions.get(uid)
    if not sess:
        await cb.message.edit_text("❌ Сессия устарела.")
        return
    if action == "cancel":
        pdf_sessions.pop(uid, None)
        await cb.message.edit_text("❌ Отменено.")
        await cb.message.answer("Главное меню:", reply_markup=kb_main())
    elif action == "all":
        await cb.message.edit_text("⏳ Записываю все транзакции...")
        saved = db.save_transactions_batch(uid, [
            {**tx, "source": "auto"}
            for tx in sess["transactions"]
        ])
        pdf_sessions.pop(uid, None)
        await cb.message.answer(f"✅ Записано {saved} транзакций!", reply_markup=kb_main())
    elif action == "review":
        sess["current_idx"] = 0
        await PDFReview.reviewing.set()
        await cb.message.edit_text("👀 Просматриваем по одной...")
        await show_pdf_tx(cb.message, uid, 0)
    await cb.answer()

async def show_pdf_tx(msg: types.Message, uid: int, idx: int):
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
    tx   = txs[idx]
    text = build_pdf_preview(tx, idx, len(txs))
    await msg.answer(text, reply_markup=kb_pdf_item(idx, len(txs)))

@dp.callback_query_handler(lambda c: c.data.startswith("pi|"), state="*")
async def cb_pdf_item(cb: types.CallbackQuery, state: FSMContext):
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
        await state.finish()
        await TxForm.waiting.set()
    elif act == "save":
        tx = txs[idx]
        db.save_transaction(uid, {**tx, "source": "auto"})
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
        await TxForm.section.set()
        await cb.message.answer(
            f"✏️ Редактирую категорию для:\n<b>{tx.get('merchant','')}</b>",
            reply_markup=kb_section(tx.get("tx_type", "Расход"))
        )
    elif act == "edit_amt":
        tx = txs[idx]
        sym = CURRENCY_SYMBOLS.get(tx.get("currency", "RUB"), "")
        await state.update_data(
            from_pdf=True, pdf_idx=idx,
            category=tx.get("category",""), section=tx.get("section",""),
            date=tx.get("date",""), tx_type=tx.get("tx_type","Расход"),
            currency=tx.get("currency","RUB"),
        )
        await remove_kb()
        await TxForm.edit_amount.set()
        await cb.message.answer(
            f"🔢 Текущая: <b>{tx.get('amount',0)} {sym}</b>\nНовая сумма:",
            reply_markup=kb_back()
        )
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Скриншот банка
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(content_types=types.ContentType.PHOTO, state="*")
async def handle_photo(msg: types.Message, state: FSMContext):
    if ALLOWED_IDS and msg.from_user.id not in ALLOWED_IDS: return
    await msg.answer("📸 Анализирую скриншот...")
    try:
        photo = msg.photo[-1]
        f     = await bot.get_file(photo.file_id)
        raw   = await bot.download_file(f.file_path)
        txs   = parse_screenshot(raw.read())
        if not txs:
            await msg.answer(
                "❌ Транзакций не найдено.\nПопробуйте скриншот списка операций.",
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

async def _send_single_tx(msg: types.Message, tx: dict):
    uid     = msg.from_user.id
    amount  = float(tx.get("amount", 0))
    cur     = tx.get("currency", "RUB")
    merchant= tx.get("merchant", "")
    tx_type = tx.get("tx_type", "Расход")
    hint    = tx.get("category_hint", "")
    date    = tx.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M")
    rate    = get_rate(cur)
    a_rub   = round(amount * rate, 2)
    sym     = CURRENCY_SYMBOLS.get(cur, cur)
    icon    = "💰" if tx_type == "Доход" else "💸"

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
    text = (
        f"📸 <b>Распознано:</b>\n"
        f"{icon} {tx_type}\n"
        f"💵 {amount:,.2f} {sym}\n"
        f"🏪 {merchant}\n"
        f"📅 {date}{hint_line}\n"
        f"📂 <b>{sec}</b> → <b>{cat}</b>\n"
        f"Подтвердите или выберите другую категорию:"
    )
    await msg.answer(text, reply_markup=kb_quick_cats(tx_id, cat, sec, tx_type))

# ══════════════════════════════════════════════════════════════════════════════
# Быстрый выбор категории (inline)
# ══════════════════════════════════════════════════════════════════════════════

@dp.callback_query_handler(lambda c: c.data.startswith("qc|"), state="*")
async def cb_quick_cat(cb: types.CallbackQuery, state: FSMContext):
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
        tx_type=tx["tx_type"], category=cat, section=sec,
        merchant=tx["m"],
    )
    data = await state.get_data()
    await TxForm.confirm.set()
    await cb.message.answer(build_preview(data), reply_markup=kb_confirm())
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("qa|"), state="*")
async def cb_quick_all(cb: types.CallbackQuery, state: FSMContext):
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
    await TxForm.section.set()
    await cb.message.answer("Выберите раздел:", reply_markup=kb_section(tx["tx_type"]))
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "qn|skip", state="*")
async def cb_quick_skip(cb: types.CallbackQuery, state: FSMContext):
    try: await cb.message.edit_reply_markup(reply_markup=None)
    except Exception: pass
    await cb.message.answer("❌ Пропущено.", reply_markup=kb_main())
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# SMS текстом
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(
    lambda m: m.text and len(m.text) > 20 and any(
        w in m.text.lower() for w in
        ["списано", "зачислено", "покупка", "оплата", "перевод",
         "баланс", "карта", "тенге", "рублей", "сом"]
    ),
    state=TxForm.waiting
)
async def handle_sms(msg: types.Message, state: FSMContext):
    if ALLOWED_IDS and msg.from_user.id not in ALLOWED_IDS: return
    await msg.answer("📱 Похоже на SMS, разбираю...")
    tx = parse_sms(msg.text)
    if not tx:
        await msg.answer("❌ Не смог распознать транзакцию.", reply_markup=kb_main())
        return
    await _send_single_tx(msg, tx)

# ══════════════════════════════════════════════════════════════════════════════
# Транзакции
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "📋 Транзакции", state="*")
async def my_txs(msg: types.Message):
    uid     = msg.from_user.id
    records = db.get_transactions(uid, limit=10)
    if not records:
        await msg.answer("📂 Нет транзакций.", reply_markup=kb_main())
        return
    text = "📋 <b>Последние 10 транзакций:</b>\n"
    for r in records:
        cur  = r.get("currency", "RUB")
        sym  = CURRENCY_SYMBOLS.get(cur, cur)
        amt  = float(r.get("amount", 0))
        a_rub = float(r.get("amount_rub", amt))
        icon = "💰" if r.get("tx_type") == "Доход" else "💸"
        d    = r.get("date", "")
        if hasattr(d, "strftime"): d = d.strftime("%d.%m.%Y")
        text += (
            f"{icon} <b>{r.get('category','')}</b>\n"
            f"  📅 {d} | {r.get('section','')}\n"
            f"  💰 {amt:,.0f} {sym}"
        )
        if cur != "RUB":
            text += f" ({a_rub:,.0f} ₽)"
        text += f"\n{'─'*28}\n"
    await msg.answer(text, reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Аналитика
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "📊 Аналитика", state="*")
async def analytics(msg: types.Message):
    uid   = msg.from_user.id
    now   = datetime.now()
    data  = db.get_monthly_summary(uid, now.year, now.month)
    if not data["sections"]:
        await msg.answer("📂 Нет данных за этот месяц.", reply_markup=kb_main())
        return
    month_name = MONTH_NAMES.get(now.month, "")
    text = f"📊 <b>Аналитика за {month_name} {now.year}:</b>\n\n"
    for row in data["sections"]:
        sec    = row.get("section", "")
        income = float(row.get("income") or 0)
        exp    = float(row.get("expense") or 0)
        assets = float(row.get("assets") or 0)
        icon   = SECTIONS.get(sec, {}).get("icon", "📂")
        total  = income + exp + assets
        text  += f"{icon} <b>{sec}</b>: {total:,.0f} ₽\n"
    text += (
        f"\n{'═'*28}\n"
        f"📈 Доходы:  {data['total_income']:,.0f} ₽\n"
        f"📉 Расходы: {data['total_expense']:,.0f} ₽\n"
        f"{'📊' if data['delta']>=0 else '⚠️'} Дельта:   {data['delta']:+,.0f} ₽\n"
    )
    if data["total_assets"]:
        text += f"🏦 Активы:  {data['total_assets']:,.0f} ₽\n"
    await msg.answer(text, reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Отложенные
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "📥 Отложенные", state="*")
async def show_drafts(msg: types.Message):
    uid    = msg.from_user.id
    drafts = db.drafts_get(uid)
    if not drafts:
        await msg.answer("📭 Нет отложенных транзакций.", reply_markup=kb_main())
        return
    text = f"📥 <b>Отложенные ({len(drafts)}):</b>\n"
    kb   = types.InlineKeyboardMarkup(row_width=1)
    for i, d in enumerate(drafts):
        sym   = CURRENCY_SYMBOLS.get(d["cur"], d["cur"])
        text += f"{i+1}. {d['a']:,.0f} {sym} — {d['m']} ({d['d']})\n"
        kb.add(types.InlineKeyboardButton(
            f"✏️ #{i+1} {d['m']} {d['a']:,.0f} {sym}",
            callback_data=f"dr|{d['id']}"
        ))
    kb.add(types.InlineKeyboardButton("🗑 Очистить все", callback_data="dr|clear"))
    await msg.answer(text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("dr|"), state="*")
async def cb_draft(cb: types.CallbackQuery, state: FSMContext):
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
    await TxForm.section.set()
    await cb.answer()

# ══════════════════════════════════════════════════════════════════════════════
# Настройки
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(lambda m: m.text == "⚙️ Настройки", state="*")
async def settings(msg: types.Message):
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
        f"🏦 Движение активов (5 категорий)\n\n"
        f"🌐 Web-интерфейс: /web",
        reply_markup=kb_main()
    )

# ══════════════════════════════════════════════════════════════════════════════
# Fallback
# ══════════════════════════════════════════════════════════════════════════════

@dp.message_handler(state="*")
async def fallback(msg: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur in (None, TxForm.waiting.state):
        await msg.answer("Используйте меню 👇", reply_markup=kb_main())

# ══════════════════════════════════════════════════════════════════════════════
# Запуск
# ══════════════════════════════════════════════════════════════════════════════

def start_bot():
    executor.start_polling(dp, skip_updates=True)

if __name__ == "__main__":
    log.info("🚀 DizelFinance Bot v3 запущен!")
    start_bot()
