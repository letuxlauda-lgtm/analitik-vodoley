import pandas as pd
from datetime import datetime, timedelta
import os

def create_inkas_report():
    """Создает текстовый отчет по инкасациям для отправки в телеграм бот"""
    
    if not os.path.exists('inki5nedel.csv'):
        return "❌ Файл inki5nedel.csv не найден!"
    
    try:
        df = pd.read_csv('inki5nedel.csv', encoding='utf-8-sig')
    except Exception as e:
        return f"❌ Ошибка загрузки файла: {e}"
    
    # Преобразуем дату и числовые колонки
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
    df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
    
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days=6)
    
    report_lines = []
    report_lines.append("📊 ОТЧЕТ ПО ИНКАСАЦИЯМ")
    report_lines.append("=" * 50)
    report_lines.append(f"Отчет сформирован: {current_date.strftime('%d.%m.%Y %H:%M')}")
    report_lines.append("")
    
    # Обрабатываем каждого техника
    technicians = [tech for tech in df['descr'].unique() if pd.notna(tech) and tech != '']
    
    for tech in technicians:
        tech_data = df[df['descr'] == tech].copy()
        tech_data = tech_data.sort_values('date')
        
        # Разделяем на сданные и на руках
        sdal_data = tech_data[tech_data['date'] <= cutoff_date]
        na_rukax_data = tech_data[tech_data['date'] > cutoff_date]
        
        report_lines.append(f"🧑‍💼 ТЕХНИК: {tech.upper()}")
        report_lines.append("-" * 40)
        
        # Сданные инкасации
        if not sdal_data.empty:
            sdal_first = sdal_data['date'].min()
            sdal_last = sdal_data['date'].max()
            bank_sdal = sdal_data['banknotes'].sum()
            coins_sdal = sdal_data['coins'].sum()
            total_sdal = bank_sdal + coins_sdal
            
            report_lines.append(f"✅ СДАНО:")
            report_lines.append(f"   Период: {sdal_first.strftime('%d.%m.%Y')} - {sdal_last.strftime('%d.%m.%Y')}")
            report_lines.append(f"   Банкноты: {bank_sdal:,.0f} грн")
            report_lines.append(f"   Монеты: {coins_sdal:,.0f} грн")
            report_lines.append(f"   ОБЩАЯ: {total_sdal:,.0f} грн (должна быть сдана)")
            report_lines.append("")
        
        # Инкасации на руках
        if not na_rukax_data.empty:
            bank_rukax = na_rukax_data['banknotes'].sum()
            coins_rukax = na_rukax_data['coins'].sum()
            total_rukax = bank_rukax + coins_rukax
            
            report_lines.append(f"💰 НА РУКАХ:")
            report_lines.append(f"   Банкноты: {bank_rukax:,.0f} грн")
            report_lines.append(f"   Монеты: {coins_rukax:,.0f} грн")
            report_lines.append(f"   ОБЩАЯ: {total_rukax:,.0f} грн")
            
            # Детализация по дням
            na_rukax_data['date_only'] = na_rukax_data['date'].dt.date
            daily_totals = na_rukax_data.groupby('date_only').agg({
                'banknotes': 'sum', 
                'coins': 'sum'
            }).reset_index()
            
            for _, day in daily_totals.iterrows():
                day_total = day['banknotes'] + day['coins']
                report_lines.append(f"   📅 {day['date_only'].strftime('%d.%m.%Y')}: {day_total:,.0f} грн")
        else:
            report_lines.append("💼 На руках инкасаций нет")
        
        report_lines.append("")
    
    # Сводная статистика
    report_lines.append("📈 СВОДНАЯ СТАТИСТИКА")
    report_lines.append("-" * 40)
    
    total_banknotes = df['banknotes'].sum()
    total_coins = df['coins'].sum()
    total_all = total_banknotes + total_coins
    
    report_lines.append(f"Общая сумма банкнот: {total_banknotes:,.0f} грн")
    report_lines.append(f"Общая сумма монет: {total_coins:,.0f} грн")
    report_lines.append(f"ОБЩИЙ ИТОГ: {total_all:,.0f} грн")
    report_lines.append("")
    report_lines.append(f"Всего инкасаций: {len(df)}")
    report_lines.append(f"Период данных: {df['date'].min().strftime('%d.%m.%Y')} - {df['date'].max().strftime('%d.%m.%Y')}")
    
    # Записываем в файл
    try:
        with open('otchet_inki.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        return "✅ Отчет успешно создан: otchet_inki.txt"
    except Exception as e:
        return f"❌ Ошибка сохранения отчета: {e}"

def get_short_report():
    """Создает краткий отчет для быстрой отправки в телеграм"""
    
    if not os.path.exists('inki5nedel.csv'):
        return "❌ Файл inki5nedel.csv не найден!"
    
    try:
        df = pd.read_csv('inki5nedel.csv', encoding='utf-8-sig')
    except Exception as e:
        return f"❌ Ошибка загрузки файла: {e}"
    
    # Преобразуем данные
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
    df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
    
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days=6)
    
    report_lines = []
    report_lines.append("📊 КРАТКИЙ ОТЧЕТ ПО ИНКАСАЦИЯМ")
    report_lines.append("=" * 35)
    
    technicians = [tech for tech in df['descr'].unique() if pd.notna(tech) and tech != '']
    
    for tech in technicians:
        tech_data = df[df['descr'] == tech].copy()
        
        na_rukax_data = tech_data[tech_data['date'] > cutoff_date]
        total_rukax = na_rukax_data['banknotes'].sum() + na_rukax_data['coins'].sum()
        
        report_lines.append(f"👤 {tech}: {total_rukax:,.0f} грн на руках")
    
    # Итоги
    total_all_rukax = df[df['date'] > cutoff_date]['banknotes'].sum() + df[df['date'] > cutoff_date]['coins'].sum()
    report_lines.append("")
    report_lines.append(f"💰 ВСЕГО НА РУКАХ: {total_all_rukax:,.0f} грн")
    report_lines.append(f"📅 Отчет на: {current_date.strftime('%d.%m.%Y %H:%M')}")
    
    return '\n'.join(report_lines)

if __name__ == "__main__":
    # Создаем полный отчет в файл
    result = create_inkas_report()
    print(result)
    
    # Выводим краткий отчет в консоль
    short_report = get_short_report()
    print("\n" + "="*50)
    print("Краткий отчет для телеграм:")
    print("="*50)
    print(short_report)