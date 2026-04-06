# ══════════════════════════════════════════════════════════════════════════════
# ВСТАВИТЬ В bot.py — после импортов добавить:
# from voice import transcribe, transcribe_fallback
#
# ВСТАВИТЬ В bot.py — после handle_photo хендлера:
# ══════════════════════════════════════════════════════════════════════════════

@dp.message(StateFilter("*"), F.voice)
async def handle_voice(msg: Message, state: FSMContext):
    """Голосовое сообщение → Groq Whisper → парсинг транзакции."""
    if not allowed(msg.from_user.id): return

    wait = await msg.answer("🎙 Слушаю голосовое...")

    try:
        # Скачиваем аудио
        f   = await bot.get_file(msg.voice.file_id)
        raw = await bot.download_file(f.file_path)
        audio_bytes = raw.read()

        # Транскрибируем через Groq
        text = transcribe(audio_bytes, filename="voice.ogg")

        # Если Groq не ответил — пробуем Faster-Whisper
        if not text:
            await msg.answer("⚠️ Groq недоступен, пробую локальный Whisper...")
            text = transcribe_fallback(audio_bytes)

        if not text:
            await msg.answer(
                "❌ Не удалось распознать голосовое.\n\n"
                "Попробуйте:\n— Говорить чётче\n— Меньше шума\n— Отправить текстом",
                reply_markup=kb_main()
            )
            return

        await msg.answer(f"🎙 Распознано:\n<i>«{text}»</i>")

        # Пытаемся распарсить как транзакцию
        from ai import parse_sms
        tx = parse_sms(text)

        if tx and tx.get("amount"):
            # Успешно распознали транзакцию
            await _send_single_tx(msg, {
                **tx,
                "date": tx.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M"),
            })
        else:
            # Не транзакция — предлагаем ручной ввод с предзаполненным текстом
            await msg.answer(
                f"Не смог распознать транзакцию из голосового.\n\n"
                f"Скажите например:\n"
                f"«Потратил пять тысяч в пятёрочке»\n"
                f"«Зачислено зарплата 150 тысяч»\n"
                f"«Заплатил за такси 800 рублей»",
                reply_markup=kb_main()
            )

    except Exception as e:
        log.error(f"handle_voice: {e}")
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())


@dp.message(StateFilter("*"), F.audio)
async def handle_audio(msg: Message, state: FSMContext):
    """Аудиофайл (mp3, wav и т.д.) — та же логика что и голосовое."""
    if not allowed(msg.from_user.id): return

    await msg.answer("🎵 Обрабатываю аудиофайл...")
    try:
        f   = await bot.get_file(msg.audio.file_id)
        raw = await bot.download_file(f.file_path)
        audio_bytes = raw.read()
        fname       = msg.audio.file_name or "audio.mp3"

        text = transcribe(audio_bytes, filename=fname)
        if not text:
            await msg.answer("❌ Не удалось распознать аудио.", reply_markup=kb_main())
            return

        await msg.answer(f"🎵 Распознано:\n<i>«{text}»</i>")
        from ai import parse_sms
        tx = parse_sms(text)
        if tx and tx.get("amount"):
            await _send_single_tx(msg, {
                **tx,
                "date": tx.get("date") or datetime.now().strftime("%d.%m.%Y, %H:%M"),
            })
        else:
            await msg.answer("Транзакция не найдена в аудио.", reply_markup=kb_main())

    except Exception as e:
        log.error(f"handle_audio: {e}")
        await msg.answer(f"❌ Ошибка: {e}", reply_markup=kb_main())
