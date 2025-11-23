import telebot
from telebot import types
import time
import logging
import sys
import os
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()
TOKEN = os.getenv("TOKEN")

if TOKEN is None:
    raise ValueError("❌ TOKEN не найден! Убедись, что он есть в файле .env")

bot = telebot.TeleBot(TOKEN)

# Настройка логирования
logging.basicConfig(level=logging.INFO, filename="bot_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

# --- ИМПОРТ РОЛЕЙ ---
try:
    import ruslan
    import dmutro
    import igor
    import calcentr
    import admin
except ImportError as e:
    print(f"⚠️ Ошибка импорта ролей: {e}")
    logging.error(f"Import error: {e}")

# Пароли для входа
PASSWORDS = {
    'rus1': 'ruslan',
    'dmu2': 'dmutro',
    'igo3': 'igor',
    'cal1': 'calcentr',
    'texd': 'texd',
    'finan': 'finance',
    'supe': 'superv',
    'adiz': 'admin'
}

# Хранилище сессий: chat_id -> module
USER_SESSIONS = {}

@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, "👋 Привет! Введите кодовое слово для авторизации:")

@bot.message_handler(func=lambda message: True)
def dispatcher(message):
    try:
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""

        # 1. Если пользователь уже авторизован
        if chat_id in USER_SESSIONS:
            role_module = USER_SESSIONS[chat_id]
            result = role_module.handle_message(bot, message)

            if result == "EXIT":
                del USER_SESSIONS[chat_id]
                bot.send_message(chat_id, "🔒 Вы вышли. Введите пароль снова:",
                                 reply_markup=types.ReplyKeyboardRemove())
            return

        # 2. Авторизация
        if text in PASSWORDS:
            role_key = PASSWORDS[text]

            if role_key == 'ruslan': USER_SESSIONS[chat_id] = ruslan
            elif role_key == 'dmutro': USER_SESSIONS[chat_id] = dmutro
            elif role_key == 'igor': USER_SESSIONS[chat_id] = igor
            elif role_key == 'calcentr': USER_SESSIONS[chat_id] = calcentr
            elif role_key == 'admin': USER_SESSIONS[chat_id] = admin
            else:
                bot.send_message(chat_id, f"⚠️ Роль '{role_key}' в разработке.")
                return

            try:
                markup = USER_SESSIONS[chat_id].get_keyboard()
                bot.send_message(chat_id, f"✅ Добро пожаловать, {role_key.upper()}!",
                                 reply_markup=markup)
            except AttributeError:
                bot.send_message(chat_id, "⚠️ Ошибка: нет функции get_keyboard() в модуле роли.")

        else:
            bot.send_message(chat_id, "⛔️ Неверный код доступа.")

    except Exception as e:
        print(f"Ошибка в диспетчере: {e}")
        logging.error(f"Dispatcher error: {e}")
        try:
            bot.send_message(message.chat.id, "Произошла ошибка. Попробуйте снова.")
        except:
            pass

if __name__ == '__main__':
    print("🤖 Бот запущен...")
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=20)
        except Exception as e:
            print(f"CRITICAL ERROR: {e}")
            time.sleep(5)
