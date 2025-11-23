from telebot import types
import utils

USER_CONTEXT = {} # Хранение состояния: 'wait_device', 'wait_client_name', 'task_type'

def get_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    markup.add('поставить задачу', 'статус выполнения задач', 'выйти с роли')
    return markup

def get_task_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add('🪙монетник🪙', '💰купюрник💰', '🖥главный модуль🖥', '‼️топим‼️', '⬜ заказ карты клиена⬜', 'свое описание')
    return markup

def handle_message(bot, message):
    chat_id = message.chat.id
    text = message.text
    state = USER_CONTEXT.get(chat_id, {}).get('step')

    # 1. Главное меню
    if text == 'поставить задачу':
        bot.send_message(chat_id, "Выберите тип проблемы:", reply_markup=get_task_menu())
        USER_CONTEXT[chat_id] = {'step': 'wait_task_type'}
        return

    elif text == 'выйти с роли':
        USER_CONTEXT.pop(chat_id, None)
        return "EXIT"

    # 2. Обработка выбора типа задачи
    if state == 'wait_task_type':
        task_type = text
        if text == 'свое описание':
             bot.send_message(chat_id, "Напишите суть проблемы:", reply_markup=types.ReplyKeyboardRemove())
             USER_CONTEXT[chat_id] = {'step': 'wait_custom_desc', 'is_card': False}
        elif text == '⬜ заказ карты клиена⬜':
             bot.send_message(chat_id, "Введите название или номер аппарата:", reply_markup=types.ReplyKeyboardRemove())
             USER_CONTEXT[chat_id] = {'step': 'wait_device', 'task_type': 'карта клиена', 'is_card': True}
        else:
             # Стандартная задача
             bot.send_message(chat_id, "Введите название или номер аппарата:", reply_markup=types.ReplyKeyboardRemove())
             USER_CONTEXT[chat_id] = {'step': 'wait_device', 'task_type': text, 'is_card': False}
        return

    # 3. Если выбрали "свое описание", ждем текст
    if state == 'wait_custom_desc':
        USER_CONTEXT[chat_id].update({'task_type': text, 'step': 'wait_device'})
        bot.send_message(chat_id, "Введите название или номер аппарата:")
        return

    # 4. Поиск аппарата и сохранение
    if state == 'wait_device':
        device_query = text
        device_data, msg = utils.smart_search_device(device_query)
        
        if not device_data:
            bot.send_message(chat_id, f"⚠️ {msg}")
            return # Ждем повторного ввода
        
        # Аппарат найден
        current_context = USER_CONTEXT[chat_id]
        tech_name = device_data['texnik'] # ruslan, dmutro, igor
        
        # Определяем таблицу по имени техника
        table_map = {'ruslan': 'zadaci_rus', 'dmutro': 'zadaci_dmu', 'igor': 'zadaci_igo'}
        target_table = table_map.get(tech_name)
        
        if not target_table:
            bot.send_message(chat_id, f"Ошибка: Техник {tech_name} не найден в системе.")
            return

        # Если это карта клиента, нужно спросить имя
        if current_context.get('is_card'):
            USER_CONTEXT[chat_id].update({
                'device_data': device_data, 
                'target_table': target_table,
                'step': 'wait_client_name'
            })
            bot.send_message(chat_id, "Введите Имя Клиента:")
            return

        # Сохраняем обычную задачу
        task_text = current_context['task_type']
        utils.add_task_to_db(target_table, device_data['id_terem'], device_data['adress'], task_text, tech_name)
        
        bot.send_message(chat_id, f"✅ Задача '{task_text}' добавлена технику {tech_name} (Аппарат: {device_data['adress']})", reply_markup=get_keyboard())
        USER_CONTEXT.pop(chat_id, None)
        return

    # 5. Если карта клиента, ждем имя
    if state == 'wait_client_name':
        client_name = text
        ctx = USER_CONTEXT[chat_id]
        task_text = f"карта клиена {client_name}"
        device_data = ctx['device_data']
        
        utils.add_task_to_db(ctx['target_table'], device_data['id_terem'], device_data['adress'], task_text, device_data['texnik'])
        
        bot.send_message(chat_id, f"✅ Заказ карты на имя {client_name} добавлен технику {device_data['texnik']}", reply_markup=get_keyboard())
        USER_CONTEXT.pop(chat_id, None)
        return