#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа сервисных записей техников.
Преобразует данные о событиях Service ON/OFF в сводную таблицу работы.
"""

import pandas as pd
from datetime import datetime, timedelta
import re
from collections import defaultdict

def extract_technician_name(event_text):
    """Извлекает имя техника из текста события"""
    if 'Service ON' in event_text:
        match = re.search(r'Service ON - (.+)', event_text)
        if match:
            return match.group(1).strip()
    return None

def process_service_data(input_file, output_file):
    """
    Обрабатывает данные сервисных записей и создает сводную таблицу.
    
    Args:
        input_file: путь к входному CSV файлу
        output_file: путь к выходному CSV файлу
    """
    
    # Загружаем данные
    print(f"Загрузка данных из {input_file}...")
    df = pd.read_csv(input_file, encoding='utf-8')
    
    # Очищаем дату от звездочек и лишних пробелов
    df['Дата'] = df['Дата'].astype(str).str.replace('*', '').str.strip()
    
    # Преобразуем дату в datetime
    df['Дата'] = pd.to_datetime(df['Дата'])
    
    # Сортируем по дате и аппарату
    df = df.sort_values(['Апарат', 'Дата'])
    
    print(f"Загружено {len(df)} записей")
    
    # Список для хранения результатов по каждому визиту
    visits = []
    
    # Группируем по аппарату для поиска пар ON/OFF
    for apparatus, group in df.groupby('Апарат'):
        group = group.sort_values('Дата')
        
        # Ищем пары Service ON - Service OFF
        on_record = None
        technician = None
        
        for idx, row in group.iterrows():
            event = row['Подія']
            
            if 'Service ON' in event:
                # Извлекаем имя техника
                tech_name = extract_technician_name(event)
                if tech_name:
                    on_record = row
                    technician = tech_name
            
            elif 'Service OFF' in event and on_record is not None:
                # Нашли пару ON/OFF
                time_on = on_record['Дата']
                time_off = row['Дата']
                duration_minutes = (time_off - time_on).total_seconds() / 60
                
                visits.append({
                    'Дата': time_on.date(),
                    'Техник': technician,
                    'Апарат': apparatus,
                    'Время начала': time_on,
                    'Время окончания': time_off,
                    'Продолжительность (мин)': round(duration_minutes, 2)
                })
                
                # Сбрасываем для следующей пары
                on_record = None
                technician = None
    
    print(f"Обработано {len(visits)} визитов")
    
    # Создаем DataFrame из визитов
    visits_df = pd.DataFrame(visits)
    
    if len(visits_df) == 0:
        print("ВНИМАНИЕ: Не найдено ни одной пары Service ON/OFF!")
        return
    
    # Создаем сводную таблицу по техникам и дням
    daily_summary = []
    
    for (date, technician), group in visits_df.groupby(['Дата', 'Техник']):
        first_time = group['Время начала'].min()
        last_time = group['Время окончания'].max()
        total_work_time = group['Продолжительность (мин)'].sum()
        apparatus_count = group['Апарат'].nunique()
        
        daily_summary.append({
            'Дата': date,
            'Техник': technician,
            'Первый раз': first_time.strftime('%H:%M:%S'),
            'Последний раз': last_time.strftime('%H:%M:%S'),
            'Работал за день (мин)': round(total_work_time, 2),
            'Количество аппаратов': apparatus_count
        })
    
    # Создаем итоговый DataFrame
    summary_df = pd.DataFrame(daily_summary)
    summary_df = summary_df.sort_values(['Дата', 'Техник'])
    
    # Сохраняем результат
    summary_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nРезультаты сохранены в {output_file}")
    
    # Также сохраняем детальную таблицу визитов
    detailed_output = output_file.replace('.csv', '_detailed.csv')
    visits_df_output = visits_df.copy()
    visits_df_output['Время начала'] = visits_df_output['Время начала'].dt.strftime('%Y-%m-%d %H:%M:%S')
    visits_df_output['Время окончания'] = visits_df_output['Время окончания'].dt.strftime('%Y-%m-%d %H:%M:%S')
    visits_df_output.to_csv(detailed_output, index=False, encoding='utf-8-sig')
    print(f"Детальная информация по визитам сохранена в {detailed_output}")
    
    # Выводим статистику
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО ДНЯМ И ТЕХНИКАМ:")
    print("="*80)
    for idx, row in summary_df.iterrows():
        print(f"\nДата: {row['Дата']}")
        print(f"Техник: {row['Техник']}")
        print(f"Первый раз появился: {row['Первый раз']}")
        print(f"Последний раз: {row['Последний раз']}")
        print(f"Работал за день: {row['Работал за день (мин)']} минут ({row['Работал за день (мин)']/60:.2f} часов)")
        print(f"Обслужено аппаратов: {row['Количество аппаратов']}")
        print("-"*80)
    
    # Общая статистика по техникам
    print("\n" + "="*80)
    print("ОБЩАЯ СТАТИСТИКА ПО ТЕХНИКАМ:")
    print("="*80)
    tech_stats = summary_df.groupby('Техник').agg({
        'Работал за день (мин)': 'sum',
        'Количество аппаратов': 'sum',
        'Дата': 'count'
    }).rename(columns={'Дата': 'Рабочих дней'})
    
    for tech, stats in tech_stats.iterrows():
        print(f"\nТехник: {tech}")
        print(f"  Всего рабочих дней: {stats['Рабочих дней']}")
        print(f"  Общее время работы: {stats['Работал за день (мин)']} мин ({stats['Работал за день (мин)']/60:.2f} часов)")
        print(f"  Всего обслужено аппаратов: {stats['Количество аппаратов']}")
        print(f"  Среднее время работы в день: {stats['Работал за день (мин)']/stats['Рабочих дней']:.2f} мин")

if __name__ == "__main__":
    input_file = "service_mes.csv"
    output_file = "texnik_za_mesyac.csv"
    
    try:
        process_service_data(input_file, output_file)
        print("\n✓ Обработка завершена успешно!")
    except Exception as e:
        print(f"\n✗ Ошибка при обработке: {e}")
        import traceback
        traceback.print_exc()
