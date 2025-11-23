import pandas as pd
import os

def process_inkas_data():
    """Обрабатывает данные инкасаций и связывает с техниками"""
    
    # Загружаем основной файл с инкасациями
    if not os.path.exists('inkas5w.csv'):
        print("❌ Файл inkas5w.csv не найден!")
        return False
    
    try:
        df_inkas = pd.read_csv('inkas5w.csv', encoding='utf-8-sig')
        print(f"✅ Загружено {len(df_inkas)} записей из inkas5w.csv")
    except Exception as e:
        print(f"❌ Ошибка загрузки inkas5w.csv: {e}")
        return False
    
    # Создаем словарь для замены по card_id
    card_tech_mapping = {
        '14147': 'ruslan',
        '23129': 'igor', 
        '9576': 'igor',
        '24662': 'dmutro'
    }
    
    print("🔧 Применяем замены по card_id...")
    
    # Применяем замены по card_id
    df_inkas['tech'] = df_inkas['card_id'].astype(str).map(card_tech_mapping)
    
    # Подсчитываем сколько записей заполнилось
    filled_by_card = df_inkas['tech'].notna().sum()
    print(f"📊 Заполнено по card_id: {filled_by_card} записей")
    
    # Если есть незаполненные записи, используем файл привязки
    if df_inkas['tech'].isna().any():
        print("🔄 Обрабатываем оставшиеся записи через файл привязки...")
        
        if os.path.exists('privyazka_aparat_texnik.csv'):
            try:
                # Загружаем файл привязки
                df_privyazka = pd.read_csv('privyazka_aparat_texnik.csv', encoding='utf-8-sig')
                print(f"✅ Загружено {len(df_privyazka)} записей из privyazka_aparat_texnik.csv")
                
                # Создаем словарь device_id -> техник из файла привязки
                # Берем первый столбец как device_id и последний как техник
                device_tech_map = {}
                
                for idx, row in df_privyazka.iterrows():
                    device_id = str(row.iloc[0]).strip()  # Первый столбец
                    tech_name = str(row.iloc[-1]).strip().lower()  # Последний столбец
                    
                    if device_id and device_id != 'nan':
                        device_tech_map[device_id] = tech_name
                
                print(f"📋 Создано {len(device_tech_map)} привязок устройств к техникам")
                
                # Заполняем оставшиеся записи по device_id
                for idx, row in df_inkas.iterrows():
                    if pd.isna(row['tech']):
                        device_id = str(row['device_id']).strip()
                        if device_id in device_tech_map:
                            df_inkas.at[idx, 'tech'] = device_tech_map[device_id]
                
                # Статистика после второго этапа
                filled_total = df_inkas['tech'].notna().sum()
                still_empty = df_inkas['tech'].isna().sum()
                print(f"📊 После привязки: заполнено {filled_total}, осталось пустых: {still_empty}")
                
            except Exception as e:
                print(f"⚠️ Ошибка обработки файла привязки: {e}")
        else:
            print("⚠️ Файл privyazka_aparat_texnik.csv не найден")
    
    # Заменяем оставшиеся NaN на пустые строки
    df_inkas['tech'] = df_inkas['tech'].fillna('')
    
    # Если в исходных данных был столбец descr, обновляем его
    if 'descr' in df_inkas.columns:
        # Объединяем старые и новые данные: где есть tech - используем его, иначе оставляем descr
        df_inkas['descr'] = df_inkas.apply(
            lambda x: x['tech'] if x['tech'] else x['descr'], 
            axis=1
        )
    else:
        # Если столбца descr нет, создаем его из tech
        df_inkas['descr'] = df_inkas['tech']
    
    # Сохраняем результат
    try:
        df_inkas.to_csv('inki5nedel.csv', index=False, encoding='utf-8-sig')
        print(f"✅ Результат сохранен в inki5nedel.csv")
        
        # Выводим статистику
        tech_stats = df_inkas[df_inkas['tech'] != '']['tech'].value_counts()
        print("\n📊 Статистика по техникам:")
        for tech, count in tech_stats.items():
            print(f"   {tech}: {count} записей")
            
        empty_count = (df_inkas['tech'] == '').sum()
        if empty_count > 0:
            print(f"   ⚠️ Не распределено: {empty_count} записей")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка сохранения файла: {e}")
        return False

if __name__ == "__main__":
    process_inkas_data()