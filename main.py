# -*- coding: utf-8 -*-
"""
DizelFinance — точка входа: запускает бот + Flask одновременно
"""

import threading
import logging
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db

log = logging.getLogger(__name__)

def run_flask():
    from web.app import app
    from config import FLASK_PORT
    log.info(f"🌐 Flask Web App → http://0.0.0.0:{FLASK_PORT}")
    app.run(host="0.0.0.0", port=FLASK_PORT, use_reloader=False, debug=False)

def run_bot():
    from bot import start_bot
    log.info("🤖 Telegram Bot запущен")
    start_bot()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )

    log.info("🚀 DizelFinance v3 — старт")
    init_db()

    # Flask в отдельном потоке
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Бот — в основном потоке
    run_bot()
