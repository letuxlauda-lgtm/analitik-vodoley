import os
import pandas as pd
import numpy as np
from datetime import datetime


class Stage8Processor:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Главный метод -----------------
    def run_stage(self):
        """Обработка данных о скорости фильтров воды"""
        try:
            self.send_progress("Этап 8/9", 0, "📊 Обработка данных скорости фильтрации...")

            if not os.path.exists('water_filter_speed.csv') or not os.path.exists('idadres.csv'):
                self.send_progress("Этап 8/9", 0, "⚠️ Отсутствуют необходимые файлы")
                return True

            water_filter = pd.read_csv('water_filter_speed.csv', encoding='utf-8-sig')
            id_adres = pd.read_csv('idadres.csv', encoding='utf-8-sig', keep_default_na=False)

            self.send_progress("Этап 8/9", 10, f"📋 Загружено {len(water_filter)} записей скорости")

            water_filter['date'] = pd.to_datetime(water_filter['date'], errors='coerce')
            water_filter = water_filter.dropna(subset=['date', 'speed'])
            
            # Приведение скорости к числовому типу, игнорируя ошибки
            water_filter['speed'] = pd.to_numeric(water_filter['speed'], errors='coerce')
            water_filter = water_filter.dropna(subset=['speed'])
            
            water_filter = water_filter.sort_values(['device_id', 'date'])

            self.send_progress("Этап 8/9", 30, "🔢 Вычисление статистик...")

            device_stats = []
            grouped = water_filter.groupby('device_id')

            for device_id, group in grouped:
                if len(group) == 0:
                    continue

                # СРЕДНЕЕ значение
                sred = group['speed'].mean()
                
                # ПОСЛЕДНЕЕ значение (самая новая запись)
                latest_row = group.sort_values('date', ascending=False).iloc[0]
                posl_znach = latest_row['speed']
                
                # Разница между средним и последним
                pokazat_skoros = sred - posl_znach

                device_stats.append({
                    'device_id': device_id,
                    'Sred': round(sred, 2),
                    'posl_znach': round(posl_znach, 2),
                    'pokazat.skoros': round(pokazat_skoros, 2)
                })

            stats_df = pd.DataFrame(device_stats)

            self.send_progress("Этап 8/9", 60, "🔗 Объединение данных...")
            
            # Убедимся, что колонка 'id' в числовом формате
            id_adres['id'] = pd.to_numeric(id_adres['id'], errors='coerce').fillna(0).astype(int)
            stats_df['device_id'] = pd.to_numeric(stats_df['device_id'], errors='coerce').fillna(0).astype(int)

            # Удаление старых колонок, если они есть
            for col in ['Sred', 'posl_znach', 'pokazat.skoros']:
                if col in id_adres.columns:
                    id_adres = id_adres.drop(columns=[col])

            # Объединение по 'id' и 'device_id'
            id_adres = id_adres.merge(stats_df, left_on='id', right_on='device_id', how='left')
            
            # Удаление вспомогательной колонки
            id_adres = id_adres.drop(columns=['device_id'])

            # Заполнение NaN для новых колонок и сохранение
            id_adres['Sred'] = id_adres['Sred'].fillna(np.nan)
            id_adres['posl_znach'] = id_adres['posl_znach'].fillna(np.nan)
            id_adres['pokazat.skoros'] = id_adres['pokazat.skoros'].fillna(np.nan)

            id_adres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            devices_with_data = stats_df['device_id'].nunique()
            self.send_progress("Этап 8/9", 100, f"✅ Обработано {devices_with_data} устройств со скоростью")

            return True

        except Exception as e:
            self.send_progress("Этап 8/9", 0, f"⚠️ Ошибка обработки скорости: {str(e)}")
            return True


if __name__ == "__main__":
    processor = Stage8Processor()
    processor.run_stage()