from telebot import types
import utils

# Для dmutro.py замените на 'zadaci_dmu', для igor.py на 'zadaci_igo'
TABLE_NAME = 'zadaci_dmu' 
ROLE_NAME = 'техник'

def get_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    markup.add('отчет для работы', 'поставленные задачи и карточки', 'выйти с роли')
    return markup

def handle_message(bot, message):
    text = message.text
    
    if text == 'отчет для работы':
        # Здесь вызов скрипта отчета (заглушка)
        bot.send_message(message.chat.id, f"🚀 Запускаю otchet_work_{TABLE_NAME.split('_')[1]}...")
        # import otchet_work_dmu; otchet_work_dmu.run()
        
    elif text == 'поставленные задачи и карточки':
        tasks = utils.get_active_tasks(TABLE_NAME)
        if not tasks:
            bot.send_message(message.chat.id, "Все задачи выполнены! 🎉")
        else:
            response = ""
            for task in tasks:
                num, id_terem, adress, zadaca, dt_start = task
                # Форматирование в зависимости от типа задачи
                if "карта клиена" in zadaca.lower():
                    icon_start, icon_mid, icon_end = "🟦", "⬜", "🟦"
                else:
                    icon_start, icon_mid, icon_end = "🟠", "🔴", "🟠"
                
                response += f"{icon_start}{num}{icon_mid}{id_terem}{icon_mid}{adress}{icon_end}{zadaca}{icon_mid}{dt_start}{icon_end}\n\n"
            
            bot.send_message(message.chat.id, response)
            bot.send_message(message.chat.id, "Чтобы закрыть задачу, напишите номер и плюс (например: 2+)")

    elif text.endswith('+') and text[:-1].isdigit():
        # Закрытие задачи
        task_num = int(text[:-1])
        success, msg = utils.close_task_db(TABLE_NAME, task_num)
        bot.send_message(message.chat.id, msg)
        
    elif text == 'выйти с роли':
        return "EXIT"
    
    return None