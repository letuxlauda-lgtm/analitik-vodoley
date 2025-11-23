import pandas as pd
import os
from datetime import datetime
import utils

def run(bot, chat_id):
    conn = utils.get_db_connection()
    tables = ['zadaci_rus', 'zadaci_dmu', 'zadaci_igo', 'zadaci_texd', 'zadaci_finan', 'zadaci_cal']
    filename = f"All_Tasks_Full_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    
    all_data = []

    try:
        for table in tables:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            if not cursor.fetchone():
                continue

            # Берем ВСЕ задачи (без фильтра по статусу)
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            
            if not df.empty:
                df['Tech_Table'] = table # Помечаем, чей это стол
                all_data.append(df)

        if not all_data:
            bot.send_message(chat_id, "📭 База данных задач пуста.")
            return

        final_df = pd.concat(all_data, ignore_index=True)
        
        # Красивая сортировка: сначала активные, потом завершенные
        final_df = final_df.sort_values(by=['status', 'date_time_start'])

        final_df.to_excel(filename, index=False)

        with open(filename, 'rb') as file:
            bot.send_document(chat_id, file, caption="🗂 Полная выгрузка всех задач (История)")

        os.remove(filename)

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {e}")
    finally:
        conn.close()