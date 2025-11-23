import pandas as pd
import sqlite3
from datetime import datetime
import os

DB_FILE = 'voda_analitik.db'
CSV_FILE = 'privyazka_aparat_texnik.csv'

def get_db_connection():
    return sqlite3.connect(DB_FILE)

def smart_search_device(query):
    """
    Умный поиск аппарата в CSV файле.
    Исправленная версия: игнорирует типы данных, пробелы и регистр.
    """
    if not os.path.exists(CSV_FILE):
        print(f"❌ Ошибка: Файл {CSV_FILE} не найден!")
        return None, "Файл привязки не найден!"
    
    try:
        # 1. Читаем всё как строки (dtype=str), чтобы "153" и 153 были равны
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig', dtype=str, keep_default_na=False)
        
        # 2. Очищаем запрос пользователя
        user_query = str(query).strip().lower()
        print(f"🔍 Поиск по запросу: '{user_query}'") # Диагностика в консоль

        # 3. Подготовка данных в таблице для поиска (создаем временные колонки для сравнения)
        # Убираем пробелы вокруг, переводим в нижний регистр
        df['id_clean'] = df['id_terem'].str.strip().str.lower()
        df['addr_clean'] = df['adress'].str.strip().str.lower()
        
        # 4. Поиск
        # А) Точное совпадение по ID (или если ID содержит этот номер)
        mask_id = df['id_clean'] == user_query
        
        # Б) Частичное совпадение по адресу
        mask_addr = df['addr_clean'].str.contains(user_query, regex=False)
        
        # Объединяем результаты
        results = df[mask_id | mask_addr]
        
        # Диагностика в консоль (показывает, сколько нашли)
        print(f"📊 Найдено совпадений: {len(results)}")

        if results.empty:
            return None, "Аппарат не найден. Попробуйте ввести только номер (например 153) или часть улицы."
        
        if len(results) > 1:
            # Если нашли больше одного (например ввели "Ленина", а там "Ленина 1" и "Ленина 5")
            # Мы попробуем найти точное совпадение среди них
            exact_match = results[results['id_clean'] == user_query]
            if len(exact_match) == 1:
                results = exact_match
            else:
                # Формируем список подсказок
                found_list = "\n".join([f"🔹 {row['id_terem']} - {row['adress']}" for index, row in results.head(5).iterrows()])
                return None, f"Найдено несколько вариантов:\n{found_list}\n🔻 Уточните запрос (введите конкретный ID)."
            
        # Возвращаем данные единственного найденного аппарата
        # Превращаем результат обратно в словарь, убирая наши служебные колонки
        found_item = results.iloc[0].to_dict()
        
        # Чистим словарь от служебных полей перед отдачей
        if 'id_clean' in found_item: del found_item['id_clean']
        if 'addr_clean' in found_item: del found_item['addr_clean']
            
        print(f"✅ Успех: {found_item['adress']} -> {found_item['texnik']}")
        return found_item, "Found"
        
    except Exception as e:
        print(f"🔥 Ошибка поиска: {e}")
        return None, f"Ошибка поиска: {e}"

def add_task_to_db(table_name, id_terem, adress, zadaca, texnik):
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"💾 Сохранение задачи в {table_name}: {zadaca}")
    
    try:
        cursor.execute(f"""
            INSERT INTO {table_name} (id_terem, adress, zadaca, texnik, date_time_start, status)
            VALUES (?, ?, ?, ?, ?, 'activ')
        """, (id_terem, adress, zadaca, texnik, now))
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")
    finally:
        conn.close()

def close_task_db(table_name, task_num):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT date_time_start FROM {table_name} WHERE num = ? AND status = 'activ'", (task_num,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return False, "Задача не найдена или уже закрыта."
    
    start_time_str = row[0]
    try:
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    except:
        # Если формат времени вдруг другой (подстраховка)
        start_time = datetime.now()

    finish_time = datetime.now()
    reaction_minutes = int((finish_time - start_time).total_seconds() / 60)
    finish_time_str = finish_time.strftime("%Y-%m-%d %H:%M:%S")
    
    cursor.execute(f"""
        UPDATE {table_name} 
        SET status = 'finish', date_time_finish = ?, vremyareakcii = ?
        WHERE num = ?
    """, (finish_time_str, reaction_minutes, task_num))
    
    conn.commit()
    conn.close()
    return True, f"Задача {task_num} закрыта. Время реакции: {reaction_minutes} мин."

def get_active_tasks(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT num, id_terem, adress, zadaca, date_time_start FROM {table_name} WHERE status='activ'")
        tasks = cursor.fetchall()
        return tasks
    except Exception as e:
        print(f"Ошибка чтения БД: {e}")
        return []
    finally:
        conn.close()