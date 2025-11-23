import os
import pandas as pd
from datetime import datetime


class Stage9Processor:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Главный метод -----------------
    def run_stage(self):
        """Добавление TDS данных из water_quality.csv в idadres.csv"""
        try:
            self.send_progress("Этап 9/9", 0, "💧 Добавление данных TDS...")

            if not os.path.exists('water_quality.csv') or not os.path.exists('idadres.csv'):
                self.send_progress("Этап 9/9", 0, "⚠️ Отсутствуют необходимые файлы")
                return True

            water_quality = pd.read_csv('water_quality.csv', encoding='utf-8-sig')
            id_adres = pd.read_csv('idadres.csv', encoding='utf-8-sig', keep_default_na=False)

            self.send_progress("Этап 9/9", 10, f"📋 Загружено {len(water_quality)} записей качества воды")

            # Преобразование даты и tds к нужным форматам
            water_quality['date'] = pd.to_datetime(water_quality['date'], errors='coerce')
            water_quality['tds'] = pd.to_numeric(water_quality['tds'], errors='coerce')

            self.send_progress("Этап 9/9", 30, "🔍 Поиск последних TDS данных...")

            # Сортировка по дате и выбор последней записи для каждого устройства
            latest_tds = water_quality.sort_values('date').groupby('device_id').last().reset_index()
            
            # Подготовка для объединения
            latest_tds['TDS'] = latest_tds['tds'].apply(lambda x: str(int(x)) if pd.notna(x) else 'Нет данных')
            latest_tds['TDSdata'] = latest_tds['date'].dt.strftime('%Y-%m-%d').fillna('Нет данных')
            
            tds_for_merge = latest_tds[['device_id', 'TDS', 'TDSdata']]

            self.send_progress("Этап 9/9", 60, "🔗 Объединение данных...")

            # Убедимся, что колонка 'id' в числовом формате
            id_adres['id'] = pd.to_numeric(id_adres['id'], errors='coerce').fillna(0).astype(int)
            tds_for_merge['device_id'] = pd.to_numeric(tds_for_merge['device_id'], errors='coerce').fillna(0).astype(int)
            
            # Удаление старых колонок, если они есть
            for col in ['TDS', 'TDSdata']:
                if col in id_adres.columns:
                    id_adres = id_adres.drop(columns=[col])

            # Объединение по 'id' и 'device_id'
            id_adres = id_adres.merge(tds_for_merge, left_on='id', right_on='device_id', how='left')
            
            # Удаление вспомогательной колонки
            id_adres = id_adres.drop(columns=['device_id'])

            # Заполнение NaN/None значений в новых колонках
            id_adres['TDS'] = id_adres['TDS'].fillna('Нет данных')
            id_adres['TDSdata'] = id_adres['TDSdata'].fillna('Нет данных')

            self.send_progress("Этап 9/9", 70, "💾 Сохранение результатов...")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f'idadres_backup_tds_{timestamp}.csv'
            id_adres.to_csv(backup_file, index=False, encoding='utf-8-sig')

            id_adres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            updated_count = id_adres[id_adres['TDS'] != 'Нет данных'].shape[0]
            self.send_progress("Этап 9/9", 100, f"✅ Обновлено TDS для {updated_count} устройств")

            return True

        except Exception as e:
            self.send_progress("Этап 9/9", 0, f"⚠️ Ошибка обработки TDS: {str(e)}")
            return True


if __name__ == "__main__":
    processor = Stage9Processor()
    processor.run_stage()