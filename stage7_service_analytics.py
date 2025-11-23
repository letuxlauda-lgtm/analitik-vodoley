import os
import pandas as pd
import re
from datetime import datetime, timedelta


class Stage7Analyzer:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    def parse_address(self, address):
        """Обработка адреса: оставляет только часть до запятой и цифры/буквы после"""
        if not address:
            return ""
        parts = address.split(',', 1)
        if len(parts) == 1:
            return parts[0].strip()
        main = parts[0].strip()
        rest = parts[1].strip()
        match = re.search(r'[\d]+[а-яА-Яa-zA-Z]?', rest)
        if match:
            return f"{main}, {match.group()}"
        return main

    def _analyze_service_data(self, service_df, texnik_df):
        """Анализ данных сервиса и создание аналитики по аппаратам"""
        service_df['Апарат_норм'] = service_df['Апарат'].apply(self.parse_address)
        
        # Подготовка данных техников
        texnik_df['adress_норм'] = texnik_df['adress'].apply(self.parse_address)
        texnik_dict = {}
        for _, row in texnik_df.iterrows():
            texnik_dict[row['adress_норм']] = row['texnik']

        results = []
        
        service_df['Дата'] = pd.to_datetime(service_df['Дата'], errors='coerce')
        service_df = service_df.dropna(subset=['Дата'])
        service_df['Дата_day'] = service_df['Дата'].dt.date
        
        # Группировка по дате и нормализованному аппарату
        grouped = service_df.groupby(['Дата_day', 'Апарат_норм'])

        for (date, aparat_norm), group in grouped:
            group_sorted = group.sort_values('Дата')
            
            on_events = group_sorted[group_sorted['Подія'].str.contains('ON', na=False, case=False)]
            off_events = group_sorted[group_sorted['Подія'].str.contains('OFF', na=False, case=False)]
            
            if len(on_events) > 0 and len(off_events) > 0:
                first_on = on_events.iloc[0]
                last_off = off_events.iloc[-1]
                
                start_dt = first_on['Дата']
                end_dt = last_off['Дата']
                
                # Защита от неправильного порядка ON/OFF
                if end_dt < start_dt:
                    continue

                start_time = start_dt.strftime('%H:%M:%S')
                end_time = end_dt.strftime('%H:%M:%S')
                
                work_minutes = int((end_dt - start_dt).total_seconds() / 60)
                
                # Определение техника из строки события
                texnik_name = ''
                if 'ON -' in first_on['Подія']:
                    texnik_match = re.search(r'ON - (.+)', first_on['Подія'])
                    if texnik_match:
                        texnik_name = texnik_match.group(1).strip()
                
                # Если не нашли в событии, ищем в таблице привязки
                if not texnik_name and aparat_norm in texnik_dict:
                    texnik_name = texnik_dict[aparat_norm]
                
                results.append({
                    'data': date,
                    'aparat': group_sorted.iloc[0]['Апарат'],
                    'start': start_time,
                    'texnik': texnik_name,
                    'end': end_time,
                    'kol-time': work_minutes,
                    'v_doroge': '',
                    'fir_point': start_time, # В оригинале тут время начала первого ON
                    'last_point': end_time    # В оригинале тут время конца последнего OFF
                })

        return pd.DataFrame(results)

    def _analyze_texnik_data(self, service_analytics):
        """Анализ данных по техникам"""
        results = []
        
        all_texniks = service_analytics['texnik'].dropna().unique()
        all_dates = service_analytics['data'].dropna().unique()
        
        for date in all_dates:
            date_analytics = service_analytics[service_analytics['data'] == date]
            
            for texnik in all_texniks:
                texnik_data = date_analytics[date_analytics['texnik'] == texnik]
                
                if len(texnik_data) > 0:
                    total_time = texnik_data['kol-time'].sum()
                    total_points = len(texnik_data)
                    travel_time = 0 # В оригинале не рассчитывается
                    
                    # Считаем общее время работы от первого ON до последнего OFF за день
                    # Для этого нужно преобразовать время в datetime-объекты
                    all_times = pd.to_datetime(texnik_data['start'], errors='coerce', format='%H:%M:%S')
                    all_times = all_times.dropna()
                    
                    if not all_times.empty:
                        first_point = all_times.min().strftime('%H:%M:%S')
                        last_point = pd.to_datetime(texnik_data['end'], errors='coerce', format='%H:%M:%S').max().strftime('%H:%M:%S')
                    else:
                        first_point = ''
                        last_point = ''

                    results.append({
                        'data': date,
                        'texnik': texnik,
                        'start': first_point,
                        'end': last_point,
                        'kol-time': total_time,
                        'v_doroge': travel_time,
                        'point': total_points
                    })
                else:
                    # В оригинале добавляются записи для техников, которые не работали
                    results.append({
                        'data': date,
                        'texnik': texnik,
                        'start': '',
                        'end': '',
                        'kol-time': '',
                        'v_dorоге': 'vuxod',
                        'point': ''
                    })
        
        return pd.DataFrame(results)

    # ----------------- Главный метод -----------------
    def run_stage(self):
        """Анализ сервисных данных и создание аналитических отчетов"""
        try:
            self.send_progress("Этап 7/9", 0, "📊 Анализ сервисных данных...")

            if not os.path.exists('service_mes.csv'):
                self.send_progress("Этап 7/9", 0, "❌ Файл service_mes.csv не найден (Нужно запустить Этап 6)")
                return False

            if not os.path.exists('privyazka_aparat_texnik.csv'):
                self.send_progress("Этап 7/9", 0, "❌ Файл privyazka_aparat_texnik.csv не найден (Требуется для привязки техников)")
                # Для упрощения я создам пустой, чтобы скрипт не упал, но в реальной системе файл должен быть
                texnik_df = pd.DataFrame(columns=['adress', 'texnik'])
                self.send_progress("Этап 7/9", 0, "⚠️ Создана пустая таблица техников. Результат будет неполным.")
            else:
                texnik_df = pd.read_csv('privyazka_aparat_texnik.csv', encoding='utf-8-sig', keep_default_na=False)


            service_df = pd.read_csv('service_mes.csv', encoding='utf-8-sig', keep_default_na=False)

            self.send_progress("Этап 7/9", 10, f"📝 Загружено {len(service_df)} записей сервиса")

            service_analytics = self._analyze_service_data(service_df, texnik_df)
            texnik_analytics = self._analyze_texnik_data(service_analytics)

            service_analytics.to_csv('ser_mes_analitik.csv', index=False, encoding='utf-8-sig')
            texnik_analytics.to_csv('tex_analitik.csv', index=False, encoding='utf-8-sig')

            self.send_progress("Этап 7/9", 100, f"✅ Аналитика создана: {len(service_analytics)} записей аппаратов, {len(texnik_analytics)} записей техников")
            return True

        except Exception as e:
            self.send_progress("Этап 7/9", 0, f"❌ Ошибка анализа сервисных данных: {str(e)}")
            return False


if __name__ == "__main__":
    analyzer = Stage7Analyzer()
    analyzer.run_stage()