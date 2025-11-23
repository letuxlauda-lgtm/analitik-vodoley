import time
import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class Stage3Api:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Методы API -----------------
    def _create_session_with_retries(self):
        session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
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
        url = "https://soliton.net.ua/water/api/devices"
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _get_water_stats(self, session, device_id, start_date, end_date):
        url = "https://soliton.net.ua/water/api/water/index.php"
        data = {"device_id": device_id, "ds": start_date, "de": end_date}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _get_water_quality(self, session, device_id, start_date, end_date):
        url = "https://soliton.net.ua/water/api/water_quality.php"
        data = {"device_id": device_id, "ds": start_date, "de": end_date}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _get_water_filter_speed(self, session, device_id, start_date, end_date):
        url = "https://soliton.net.ua/water/api/water_filter_speed.php"
        data = {"device_id": device_id, "ds": start_date, "de": end_date}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _get_device_inkas(self, session, device_id, start_date, end_date):
        url = "https://soliton.net.ua/water/api/device_inkas.php"
        data = {"device_id": device_id, "ds": start_date, "de": end_date}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _get_device_sensors(self, session, device_id):
        url = "https://soliton.net.ua/water/api/device_sensors.php"
        data = {"device_id": device_id}
        try:
            response = session.post(url, json=data, timeout=30)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None

    def _save_all_reports(self, results, devices_list):
        """Сохранение отчетов в CSV"""
        # Devices
        if devices_list:
            pd.DataFrame(devices_list).to_csv("devices.csv", index=False, encoding="utf-8-sig")

        # Sensors
        sensors_data = []
        for device_id, sensors in results["sensors"].items():
            if sensors and sensors.get("status") == "success" and sensors.get("data"):
                for item in sensors["data"]:
                    sensors_data.append({
                        "device_id": device_id,
                        "address": sensors.get("address", ""),
                        "date": item.get("date", ""),
                        "name": item.get("name", ""),
                        "state": item.get("state", ""),
                        "sens_val": item.get("sens_val", ""),
                        "descr": item.get("descr", "")
                    })

        if sensors_data:
            pd.DataFrame(sensors_data).to_csv("device_sensors.csv", index=False, encoding='utf-8-sig')

        # Water Filter Speed
        filter_speed_data = []
        for device_id, filter_data in results["filter_speed"].items():
            if filter_data and filter_data.get("status") == "success" and filter_data.get("data"):
                for item in filter_data["data"]:
                    filter_speed_data.append({
                        "device_id": device_id,
                        "address": filter_data.get("address", ""),
                        "date": item.get("date", ""),
                        "speed": item.get("speed", "")
                    })

        if filter_speed_data:
            pd.DataFrame(filter_speed_data).to_csv("water_filter_speed.csv", index=False, encoding='utf-8-sig')

        # Water Quality (TDS)
        quality_data = []
        for device_id, quality in results["water_quality"].items():
            if quality and quality.get("status") == "success" and quality.get("data"):
                for item in quality["data"]:
                    quality_data.append({
                        "device_id": device_id,
                        "address": quality.get("address", ""),
                        "date": item.get("date", ""),
                        "tds": item.get("tds", "")
                    })

        if quality_data:
            pd.DataFrame(quality_data).to_csv("water_quality.csv", index=False, encoding='utf-8-sig')
            
        # Water Stats (можно сохранить для отладки, хотя они не используются в последующих этапах)
        stats_data = []
        for device_id, stats in results["water_stats"].items():
            if stats and stats.get("status") == "success" and stats.get("data"):
                for item in stats["data"]:
                    stats_data.append({
                        "device_id": device_id,
                        "address": stats.get("address", ""),
                        **item
                    })
        if stats_data:
            pd.DataFrame(stats_data).to_csv("water_stats.csv", index=False, encoding='utf-8-sig')

        # Inkas (можно сохранить для отладки)
        inkas_data = []
        for device_id, inkas in results["inkas"].items():
            if inkas and inkas.get("status") == "success" and inkas.get("data"):
                for item in inkas["data"]:
                    inkas_data.append({
                        "device_id": device_id,
                        "address": inkas.get("address", ""),
                        **item
                    })
        if inkas_data:
            pd.DataFrame(inkas_data).to_csv("device_inkas.csv", index=False, encoding='utf-8-sig')


    # ----------------- Главный метод -----------------
    def run_stage(self):
        try:
            self.send_progress("Этап 3/9", 0, "🌐 Создание API сессии...")

            session = self._create_session_with_retries()

            self.send_progress("Этап 3/9", 10, "📋 Получение списка аппаратов...")
            devices_response = self._get_all_devices(session)

            if not devices_response or devices_response.get("status") != "success":
                self.send_progress("Этап 3/9", 0, "❌ Не удалось получить список аппаратов")
                return False

            devices_list = devices_response.get("devices", [])
            self.send_progress("Этап 3/9", 20, f"📊 Найдено {len(devices_list)} аппаратов")

            today = datetime.now().date()
            start_date_short = (today - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            end_date_short = today.strftime('%Y-%m-%d 23:59:59')
            start_date_long = (today - timedelta(days=180)).strftime('%Y-%m-%d')
            end_date_long = today.strftime('%Y-%m-%d')

            results = {
                "water_stats": {},
                "water_quality": {},
                "filter_speed": {},
                "inkas": {},
                "sensors": {}
            }

            total_devices = len(devices_list)
            for i, device in enumerate(devices_list):
                device_id = device['id']
                progress = 20 + int((i / total_devices) * 60) if total_devices else 20
                self.send_progress("Этап 3/9", progress, f"📊 Обработка аппарата {device_id} ({i+1}/{total_devices})")
                
                # water_quality (нужно для этапа 9) - длительный период
                results["water_quality"][device_id] = self._get_water_quality(session, device_id, start_date_long, end_date_long)
                time.sleep(0.15)
                
                # filter_speed (нужно для этапа 8) - короткий период
                results["filter_speed"][device_id] = self._get_water_filter_speed(session, device_id, start_date_short, end_date_short)
                time.sleep(0.15)

                # sensors (нужно для этапа 4) - только текущие данные, без ds/de
                results["sensors"][device_id] = self._get_device_sensors(session, device_id)
                time.sleep(0.15)

                # water_stats, inkas - не нужны для последующих этапов, но собираются в оригинале
                results["water_stats"][device_id] = self._get_water_stats(session, device_id, start_date_short, end_date_short)
                time.sleep(0.15)
                results["inkas"][device_id] = self._get_device_inkas(session, device_id, start_date_short, end_date_short)
                time.sleep(0.15)


            self.send_progress("Этап 3/9", 85, "💾 Сохранение данных...")
            self._save_all_reports(results, devices_list)

            self.send_progress("Этап 3/9", 100, "✅ API данные собраны")
            return True

        except Exception as e:
            self.send_progress("Этап 3/9", 0, f"❌ Ошибка: {str(e)}")
            return False


if __name__ == "__main__":
    parser = Stage3Api()
    parser.run_stage()