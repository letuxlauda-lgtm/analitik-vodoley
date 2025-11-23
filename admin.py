import threading
import time
import os
from telebot import types

# --- ИМПОРТЫ МОДУЛЕЙ ---
# 1. Парсер рабочий
try:
    import parse_work
except ImportError:
    parse_work = None

# 2. Парсер инкассаций
try:
    import parse_ink
except ImportError:
    parse_ink = None

# 3. Парсер сервиса (НОВЫЙ)
try:
    import parse_service
except ImportError:
    parse_service = None

# 4. Отчеты и задачи
try:
    import otchet_work
    import all_zadaci
except ImportError:
    pass

# --- КЛАВИАТУРА ---
def get_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    btn1 = types.KeyboardButton('парсинг рабочий')
    btn2 = types.KeyboardButton('парсинг инкасаций')
    btn3 = types.KeyboardButton('парсинг сервиса') # НОВАЯ КНОПКА
    
    btn4 = types.KeyboardButton('отчет для работы')
    btn5 = types.KeyboardButton('отчет по инкасациям')
    btn6 = types.KeyboardButton('отчет по сервису') # НОВАЯ КНОПКА
    
    btn7 = types.KeyboardButton('все задачи')
    btn8 = types.KeyboardButton('выйти с роли')
    
    markup.add(btn1, btn2, btn3)
    markup.add(btn4, btn5, btn6)
    markup.add(btn7, btn8)
    return markup

# --- УНИВЕРСАЛЬНЫЙ ЗАПУСКАТЕЛЬ В ПОТОКЕ ---
def launch_process_in_thread(bot, chat_id, worker_func, start_message_text):
    try:
        msg = bot.send_message(chat_id, f"⏳ {start_message_text}")
        message_id = msg.message_id
    except Exception as e:
        print(f"Не удалось отправить сообщение: {e}")
        return
    
    last_text_container = {"text": ""}

    def telegram_callback(text_message):
        if text_message == last_text_container["text"]: return 
        try:
            display_text = (text_message[:200] + '...') if len(text_message) > 200 else text_message
            bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"⏳ {display_text}")
            last_text_container["text"] = text_message
        except Exception: pass

    def thread_target():
        try:
            worker_func(telegram_callback)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Ошибка в процессе: {e}")

    threading.Thread(target=thread_target, daemon=True).start()


# --- ОБРАБОТЧИК СООБЩЕНИЙ ---
def handle_message(bot, message):
    text = message.text
    chat_id = message.chat.id
    
    # ================= ПАРСИНГ РАБОЧИЙ =================
    if text == 'парсинг рабочий':
        if not parse_work:
            bot.send_message(chat_id, "❌ Модуль parse_work.py не найден.")
            return
        launch_process_in_thread(bot, chat_id, 
            lambda cb: parse_work.run_full_cycle(callback=cb), 
            "Запуск рабочего парсера...")

    # ================= ПАРСИНГ ИНКАССАЦИЙ =================
    elif text == 'парсинг инкасаций':
        if not parse_ink:
            bot.send_message(chat_id, "❌ Модуль parse_ink.py не найден.")
            return
        
        def worker(callback):
            success = parse_ink.run_full_cycle(callback=callback)
            if success:
                callback("✅ Готово! Отправляю отчет...")
                time.sleep(0.5)
                bot.send_message(chat_id, parse_ink.get_final_report_text())
                try:
                    with open('otchet_inki.txt', 'rb') as f:
                        bot.send_document(chat_id, f, caption="Полный отчет (файл)")
                except: pass
            else:
                callback("❌ Процесс завершился с ошибкой.")

        launch_process_in_thread(bot, chat_id, worker, "Запуск парсера инкассаций...")

    # ================= ПАРСИНГ СЕРВИСА (НОВОЕ) =================
    elif text == 'парсинг сервиса':
        if not parse_service:
            bot.send_message(chat_id, "❌ Модуль parse_service.py не найден.")
            return
        
        def worker(callback):
            success = parse_service.run_full_cycle(callback=callback)
            if success:
                callback("✅ Готово! Файлы сформированы.")
                time.sleep(0.5)
                # Отправляем файлы
                try:
                    if os.path.exists('otchet_service.txt'):
                        with open('otchet_service.txt', 'rb') as f:
                            bot.send_document(chat_id, f, caption="📄 Лог сервиса")
                    
                    if os.path.exists('interactive_routes_map.html'):
                        with open('interactive_routes_map.html', 'rb') as f:
                            bot.send_document(chat_id, f, caption="🗺️ Интерактивная карта")
                except Exception as e:
                    bot.send_message(chat_id, f"Ошибка отправки файлов: {e}")
            else:
                callback("❌ Ошибка при парсинге сервиса.")

        launch_process_in_thread(bot, chat_id, worker, "Запуск парсера сервиса...")

    # ================= ОТЧЕТЫ =================
    elif text == 'отчет для работы':
        bot.send_message(chat_id, "📊 Формирую отчет...")
        try:
            if 'otchet_work' in globals():
                threading.Thread(target=otchet_work.run, args=(bot, chat_id, None)).start()
            else: bot.send_message(chat_id, "Скрипт otchet_work.py не найден.")
        except Exception as e: bot.send_message(chat_id, f"Ошибка: {e}")

    elif text == 'отчет по инкасациям':
        try:
            with open('otchet_inki.txt', 'rb') as f:
                bot.send_document(chat_id, f, caption="📂 Последний отчет по инкассациям")
        except FileNotFoundError:
             bot.send_message(chat_id, "❌ Отчет еще не сформирован.")

    elif text == 'отчет по сервису':
        # Отправка двух файлов
        files_sent = 0
        try:
            if os.path.exists('otchet_service.txt'):
                with open('otchet_service.txt', 'rb') as f:
                    bot.send_document(chat_id, f, caption="📄 Отчет по сервису")
                    files_sent += 1
            if os.path.exists('interactive_routes_map.html'):
                with open('interactive_routes_map.html', 'rb') as f:
                    bot.send_document(chat_id, f, caption="🗺️ Карта маршрутов")
                    files_sent += 1
            
            if files_sent == 0:
                bot.send_message(chat_id, "❌ Файлы отчетов не найдены. Запустите парсинг сервиса.")
        except Exception as e:
            bot.send_message(chat_id, f"❌ Ошибка: {e}")

    # ================= ДРУГОЕ =================
    elif text == 'все задачи':
        bot.send_message(chat_id, "📋 Выгружаю базу данных...")
        try:
            if 'all_zadaci' in globals():
                threading.Thread(target=all_zadaci.run, args=(bot, chat_id)).start()
            else: bot.send_message(chat_id, "Скрипт all_zadaci.py не найден.")
        except Exception as e: bot.send_message(chat_id, f"Ошибка: {e}")

    elif text == 'выйти с роли':
        return "EXIT"
    
    else:
        bot.send_message(chat_id, "Неизвестная команда.")

    return None