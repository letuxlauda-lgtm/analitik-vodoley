import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class Stage10InkasProcessor:
    def __init__(self, callback=None):
        """
        callback - функция для отправки прогресса: callback(stage, progress, message)
        """
        self.callback = callback
        self.INKAS_FILENAME = 'inkas5w.csv'
        self.PROCESSED_FILENAME = 'inkas5w_processed.csv'
        self.PRIVYAZKA_FILENAME = 'privyazka_tex_adres.csv'

    def send_progress(self, stage, progress, message):
        """Отправка прогресса выполнения"""
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    def _create_session_with_retries(self):
        """Создает сессию requests с стратегией повторных попыток"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://soliton.net.ua'
        })
        return session

    def _get_all_devices(self, session):
        """Получение списка всех аппаратов"""
        url = "https://soliton.net.ua/water/api/devices"
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            self.send_progress("API", 0, f"❌ Ошибка при получении списка аппаратов: {e}")
        return None

    def _get_device_inkas(self, session, device_id, start_date, end_date):
        """Получение данных по инкасациям"""
        url = "https://soliton.net.ua/water/api/device_inkas.php"
        data = {"device_id": device_id, "ds": start_date, "de": end_date}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            self.send_progress("API", 0, f"⚠️ Ошибка при запросе инкасации {device_id}: {e}")
        return None

    def _collect_inkas_data(self):
        """Собирает данные инкасаций с API и сохраняет их в CSV."""
        stage_name = "Этап 10.1/10"
        self.send_progress(stage_name, 0, "🌐 Инициализация API сессии и сбор данных...")
        
        session = self._create_session_with_retries()
        devices_response = self._get_all_devices(session)
        
        if not devices_response or devices_response.get("status") != "success":
            self.send_progress(stage_name, 0, "❌ Не удалось получить список аппаратов")
            return False
        
        devices_list = devices_response.get("devices", [])
        self.send_progress(stage_name, 10, f"📋 Найдено аппаратов: {len(devices_list)}")
        
        today = datetime.now().date()
        start_date = (today - timedelta(weeks=5)).strftime('%Y-%m-%d 00:00:00')
        end_date = today.strftime('%Y-%m-%d 23:59:59')
        
        self.send_progress(stage_name, 20, f"📅 Период парсинга: с {start_date} по {end_date}")
        
        inkas_data = []
        total_devices = len(devices_list)

        for i, device in enumerate(devices_list):
            device_id = device['id']
            progress = 20 + int((i / total_devices) * 60)
            self.send_progress(stage_name, progress, f"📊 Сбор инкасаций для {device_id} ({i+1}/{total_devices})")
            
            inkas = self._get_device_inkas(session, device_id, start_date, end_date)
            time.sleep(0.3)
            
            if inkas and inkas.get("status") == "success" and inkas.get("data"):
                for item in inkas["data"]:
                    inkas_data.append({
                        "device_id": device_id,
                        "address": inkas.get("address", ""),
                        "date": item.get("date", ""),
                        "card_id": item.get("card_id", ""),
                        "sum": item.get("sum", ""),
                        "banknotes": item.get("banknotes", ""),
                        "coins": item.get("coins", ""),
                        "descr": item.get("descr", "")
                    })
        
        if inkas_data:
            df = pd.DataFrame(inkas_data)
            df.to_csv(self.INKAS_FILENAME, index=False, encoding="utf-8-sig")
            self.send_progress(stage_name, 90, f"✅ Создан файл {self.INKAS_FILENAME} с {len(inkas_data)} записями")
            return True
        else:
            self.send_progress(stage_name, 90, "❌ Не удалось собрать данные по инкасациям")
            return False

    def _process_inkas_data(self):
        """Обрабатывает собранные данные инкасаций и связывает их с техниками."""
        stage_name = "Этап 10.2/10"
        self.send_progress(stage_name, 0, f"📂 Загрузка данных: {self.INKAS_FILENAME} и {self.PRIVYAZKA_FILENAME}...")

        if not os.path.exists(self.INKAS_FILENAME):
            self.send_progress(stage_name, 0, f"❌ Файл {self.INKAS_FILENAME} не найден. Запустите сначала сбор данных.")
            return False
        
        try:
            df_inkas = pd.read_csv(self.INKAS_FILENAME, encoding='utf-8-sig', keep_default_na=False)
        except Exception as e:
            self.send_progress(stage_name, 0, f"❌ Ошибка чтения {self.INKAS_FILENAME}: {e}")
            return False

        if not os.path.exists(self.PRIVYAZKA_FILENAME):
            self.send_progress(stage_name, 0, f"⚠️ Файл {self.PRIVYAZKA_FILENAME} не найден. Связывание не будет выполнено.")
            df_privyazka = pd.DataFrame(columns=['id', 'texnik'])
        else:
            try:
                df_privyazka = pd.read_csv(self.PRIVYAZKA_FILENAME, encoding='utf-8-sig', keep_default_na=False)
            except Exception as e:
                self.send_progress(stage_name, 0, f"❌ Ошибка чтения {self.PRIVYAZKA_FILENAME}: {e}")
                df_privyazka = pd.DataFrame(columns=['id', 'texnik'])
        
        self.send_progress(stage_name, 20, "🔄 Обработка и стандартизация данных...")
        
        # Словарь для замены некорректных значений
        replacements = {
            'Р†РіРѕСЂ': 'igor',
            'Р”РјРёС‚СЂРѕ': 'dmutro', 
            'Р СѓСЃР»Р°РЅ': 'ruslan',
            'Игорь': 'igor',
            'Дмитро': 'dmutro',
            'Руслан': 'ruslan'
        }

        # Применяем замены и стандартизацию
        df_inkas['descr'] = df_inkas['descr'].astype(str).str.strip().replace(replacements, regex=False)
        df_inkas['descr'] = df_inkas['descr'].str.lower().str.strip()

        # Создаем словарь для сопоставления device_id с техником
        device_to_tech = {}
        if not df_privyazka.empty and 'id' in df_privyazka.columns and 'texnik' in df_privyazka.columns:
            df_privyazka['id'] = pd.to_numeric(df_privyazka['id'], errors='coerce').fillna(0).astype(int)
            df_privyazka['texnik'] = df_privyazka['texnik'].astype(str).str.lower().str.strip()
            device_to_tech = df_privyazka.set_index('id')['texnik'].to_dict()

        self.send_progress(stage_name, 50, "🔗 Связывание данных с техниками...")
        
        # Заполняем пустые/некорректные значения descr из привязки
        for idx, row in df_inkas.iterrows():
            descr = str(row['descr']).strip()
            
            # Если descr пустое или неизвестное значение
            if not descr or descr in ['nan', 'none', 'null', ''] or descr not in ['igor', 'dmutro', 'ruslan']:
                device_id = pd.to_numeric(row['device_id'], errors='coerce')
                
                if pd.notna(device_id):
                    device_id_int = int(device_id)
                    tech_name = device_to_tech.get(device_id_int, '')
                    
                    if tech_name:
                        df_inkas.at[idx, 'descr'] = tech_name

        # Финальная очистка
        df_inkas['descr'] = df_inkas['descr'].replace({
            'nan': '', 
            'none': '', 
            'null': ''
        }).fillna('').astype(str).str.strip()

        # Сохраняем результат
        df_inkas.to_csv(self.PROCESSED_FILENAME, index=False, encoding='utf-8-sig')

        self.send_progress(stage_name, 90, f"💾 Результат сохранен в {self.PROCESSED_FILENAME}")

        # Статистика
        stats = df_inkas['descr'].value_counts(dropna=False)
        self.send_progress(stage_name, 95, f"📊 Статистика по техникам: {dict(stats)}")

        self.send_progress(stage_name, 100, "✅ Обработка завершена")
        return True

    def run_stage(self):
        """Запускает полный цикл сбора и обработки инкасаций"""
        self.send_progress("Этап 10", 0, "🚀 Начало цикла инкасаций...")
        
        # Сбор данных
        success = self._collect_inkas_data()
        
        # Обработка данных (только если сбор успешен)
        if success:
            self._process_inkas_data()
        
        self.send_progress("Этап 10", 100, "🎉 Этап 10 завершен!")
        return success

if __name__ == "__main__":
    processor = Stage10InkasProcessor()
    processor.run_stage()