import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class Stage4Processor:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Главный метод -----------------
    def run_stage(self):
        """Определение статусов DV1, DV2, DV3"""
        try:
            self.send_progress("Этап 4/9", 0, "📂 Загрузка файлов: device_sensors.csv и idadres.csv...")

            if not os.path.exists('device_sensors.csv') or not os.path.exists('idadres.csv'):
                self.send_progress("Этап 4/9", 0, "❌ Отсутствуют необходимые файлы")
                return False

            sensors_df = pd.read_csv('device_sensors.csv', encoding='utf-8-sig')
            idadres_df = pd.read_csv('idadres.csv', encoding='utf-8-sig', keep_default_na=False)

            self.send_progress("Этап 4/9", 10, f"📊 Загружено: {len(sensors_df)} записей сенсоров")

            # Поиск столбца ID, т.к. его название может быть 'id' или 'ID'
            id_column = next((col for col in idadres_df.columns if col.upper() == 'ID'), None)

            if id_column is None:
                self.send_progress("Этап 4/9", 0, "❌ Не найден столбец ID в idadres.csv")
                return False

            # Создание колонок статусов, если их нет
            for col in ['dv1r', 'dv2r', 'dv3r']:
                if col not in idadres_df.columns:
                    idadres_df[col] = 'nerabotaet'

            self.send_progress("Этап 4/9", 20, "📅 Проверка дат сенсоров...")

            sensors_df['date'] = pd.to_datetime(sensors_df['date'], errors='coerce')
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            def is_recent_date(date_val):
                try:
                    if pd.isna(date_val):
                        return False
                    # Преобразование в дату, если это не datetime.date объект
                    date_obj = date_val.date() if hasattr(date_val, 'date') else pd.to_datetime(date_val).date()
                    return date_obj == today or date_obj == yesterday
                except Exception:
                    return False

            # Подготовка для сопоставления ID
            sensors_df['device_id_str'] = sensors_df['device_id'].astype(str)
            idadres_df['id_str'] = idadres_df[id_column].astype(str)

            total_devices = len(idadres_df)
            for index, row in idadres_df.iterrows():
                progress = 20 + int((index / total_devices) * 70) if total_devices else 20
                device_id_str = row['id_str']

                self.send_progress("Этап 4/9", progress, f"🔍 Проверка аппарата {device_id_str} ({index+1}/{total_devices})")

                device_sensors = sensors_df[sensors_df['device_id_str'] == device_id_str]

                if len(device_sensors) == 0:
                    continue

                for sensor, col in [('dv1', 'dv1r'), ('dv2', 'dv2r'), ('dv3', 'dv3r')]:
                    sensor_records = device_sensors[device_sensors['name'].str.lower() == sensor]
                    if len(sensor_records) > 0:
                        # Проверяем, есть ли записи за сегодня или вчера
                        recent_records = sensor_records[sensor_records['date'].apply(is_recent_date)]
                        if len(recent_records) > 0:
                            idadres_df.at[index, col] = 'rabotaet'

            # Удаление вспомогательного столбца
            idadres_df = idadres_df.drop(columns=['id_str'])

            # Сохранение с бэкапом
            backup_file = f'idadres_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            idadres_df.to_csv(backup_file, index=False, encoding='utf-8-sig')

            idadres_df.to_csv('idadres.csv', index=False, encoding='utf-8-sig')
            self.send_progress("Этап 4/9", 100, f"✅ Обновлено статусов для {total_devices} аппаратов")

            return True

        except Exception as e:
            self.send_progress("Этап 4/9", 0, f"❌ Ошибка: {str(e)}")
            return False


if __name__ == "__main__":
    processor = Stage4Processor()
    processor.run_stage()