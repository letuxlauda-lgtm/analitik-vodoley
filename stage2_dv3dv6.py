import time
import re
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


class Stage2Parser:
    def __init__(self, callback=None):
        self.callback = callback
        self.driver = None
        self.wait = None

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Утилиты для работы с Selenium и Fatal Error -----------------
    def is_fatal_page(self):
        try:
            html = self.driver.page_source.lower()
            if "fatal error" in html or "allowed memory size" in html or "memory size" in html:
                return True
        except Exception:
            pass
        return False

    def set_today_dates_on_page(self):
        today = datetime.now()
        day = str(today.day)
        month = str(today.month)
        year = str(today.year)

        names = [
            ('date_day_start', day), ('date_month_start', month), ('date_year_start', year),
            ('date_day_end', day), ('date_month_end', month), ('date_year_end', year),
            ('date_month_start', month), ('date_month_end', month),
            ('date_day_start', day), ('date_day_end', day)
        ]
        changed = False
        for name, value in names:
            try:
                select_el = self.driver.find_element(By.NAME, name)
                Select(select_el).select_by_value(value)
                changed = True
                time.sleep(0.15)
            except (NoSuchElementException, Exception):
                continue
        
        input_names = ['date_start', 'date_end', 'date_ds', 'date_de']
        for iname in input_names:
            try:
                inp = self.driver.find_element(By.NAME, iname)
                try:
                    inp.clear()
                    inp.send_keys(f"{today.strftime('%Y-%m-%d')}")
                    changed = True
                except Exception:
                    pass
            except NoSuchElementException:
                continue
        return changed

    def try_back_and_fix_dates(self):
        try:
            try:
                self.driver.back()
            except Exception:
                try:
                    self.driver.execute_script('window.history.back()')
                except Exception:
                    pass

            time.sleep(1.2)
            self.set_today_dates_on_page()
            time.sleep(0.8)
            return not self.is_fatal_page()
        except Exception:
            return False

    def safe_get(self, url, wait_seconds=2):
        try:
            self.driver.get(url)
            time.sleep(wait_seconds)
            if self.is_fatal_page():
                self.send_progress("Система", 0, "⚠️ Обнаружен Fatal error при get — делаю Back и меняю даты")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    try:
                        self.driver.get(url)
                        time.sleep(wait_seconds)
                    except Exception:
                        pass
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry страница снова Fatal — пропускаем шаг")
                        return False
            return True
        except WebDriverException as e:
            self.send_progress("Система", 0, f"❌ WebDriverException в safe_get: {e}")
            return False
        except Exception as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_get: {e}")
            return False

    def safe_find_and_click(self, by, value, wait_after=1.0):
        try:
            elem = self.wait.until(EC.presence_of_element_located((by, value)))
            elem.click()
            time.sleep(wait_after)
            if self.is_fatal_page():
                self.send_progress("Система", 0, "⚠️ Обнаружен Fatal error после click — назад и смена дат")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    try:
                        elem = self.wait.until(EC.presence_of_element_located((by, value)))
                        elem.click()
                        time.sleep(wait_after)
                    except Exception:
                        pass
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry click страница снова Fatal — пропускаем шаг")
                        return False
            return True
        except TimeoutException:
            self.send_progress("Система", 0, f"⚠️ Элемент не найден: {by} {value}")
            return False
        except WebDriverException as e:
            self.send_progress("Система", 0, f"❌ WebDriverException в safe_find_and_click: {e}")
            return False
        except Exception as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_find_and_click: {e}")
            return False

    # ----------------- Утилиты парсинга -----------------
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

    # ----------------- Главный метод -----------------
    def run_stage(self):
        self.send_progress("Этап 2/9", 0, "🔍 Инициализация браузера...")
        
        try:
            # Инициализация для переиспользования в случае Fatal Error
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 15)
        except Exception as e:
            self.send_progress("Этап 2/9", 0, f"❌ Не удалось инициализировать WebDriver: {e}")
            return False

        try:
            self.send_progress("Этап 2/9", 5, "🔗 Переход к датчикам...")
            # Требуется повторная авторизация, т.к. скрипт отдельный
            if not self.safe_get("https://soliton.net.ua/water/baza/"):
                self.send_progress("Этап 2/9", 0, "⚠️ Пропускаю авторизацию из-за Fatal страницы")
                return False

            time.sleep(1)
            try:
                self.driver.find_element(By.NAME, "auth_login").send_keys("Service_zenya")
                self.driver.find_element(By.NAME, "auth_pass").send_keys("zenya")
                submit = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
                submit.click()
                time.sleep(1.2)
                self.wait.until(EC.presence_of_element_located((By.XPATH, "//a[@href='/water/baza/?fid=2&subsection=stat']")))
            except Exception as e:
                self.send_progress("Этап 2/9", 0, f"❌ Не удалось авторизоваться: {e}")
                return False

            if not self.safe_find_and_click(By.XPATH, "//a[@href='/water/baza/?section=sensors&fid=2']", wait_after=1.2):
                self.send_progress("Этап 2/9", 0, "⚠️ Не удалось перейти к датчикам — пропускаю этап")
                return True

            time.sleep(1.2)

            self.send_progress("Этап 2/9", 10, "🎛️ Парсинг датчика DV3...")
            try:
                sensor_select = Select(self.driver.find_element(By.NAME, "sensor"))
                sensor_select.select_by_value("dv3")
                time.sleep(0.3)
            except Exception:
                self.send_progress("Этап 2/9", 0, "⚠️ Не удалось выбрать dv3 — пропускаю")
                return True

            if not self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=15):
                self.send_progress("Этап 2/9", 0, "⚠️ Не удалось получить таблицу DV3 — пропускаю")
                return True

            try:
                table = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//table[.//th[contains(text(), 'Дата')] and .//th[contains(text(), 'Датчик')]]"))
                )
                rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
            except TimeoutException:
                self.send_progress("Этап 2/9", 0, "⚠️ Таблица DV3 не найдена после ожидания")
                rows = []

            dv3_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 5:
                    dv3_data.append({
                        'datetime': cells[0].text.strip(),
                        'sensor': cells[1].text.strip(),
                        'state': cells[2].text.strip(),
                        'value': cells[3].text.strip(),
                        'apparatus': self.parse_address(cells[4].text.strip())
                    })

            df_dv3 = pd.DataFrame(dv3_data)

            if not df_dv3.empty:
                df_dv3['datetime'] = df_dv3['datetime'].apply(lambda x: re.sub(r'[*\s]+', ' ', str(x)).strip())
                df_dv3['datetime'] = pd.to_datetime(df_dv3['datetime'], errors='coerce')
                df_dv3 = df_dv3.dropna(subset=['datetime'])
                df_dv3 = df_dv3.sort_values(['apparatus', 'datetime']).reset_index(drop=True)
                df_dv3.to_csv('dv3dv.csv', index=False, encoding='utf-8-sig')
                self.send_progress("Этап 2/9", 50, f"✅ DV3: сохранено {len(dv3_data)} записей")
            else:
                self.send_progress("Этап 2/9", 50, "✅ DV3: данных не найдено")

            time.sleep(1.2)

            self.send_progress("Этап 2/9", 60, "🎛️ Парсинг датчика DV6...")
            try:
                sensor_select = Select(self.driver.find_element(By.NAME, "sensor"))
                sensor_select.select_by_value("dv6")
                time.sleep(0.3)
            except Exception:
                self.send_progress("Этап 2/9", 0, "⚠️ Не удалось выбрать dv6 — пропускаю")
                return True

            if not self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=15):
                self.send_progress("Этап 2/9", 0, "⚠️ Не удалось получить таблицу DV6 — пропускаю")
                return True

            try:
                table = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//table[.//th[contains(text(), 'Дата')] and .//th[contains(text(), 'Датчик')]]"))
                )
                rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
            except TimeoutException:
                self.send_progress("Этап 2/9", 0, "⚠️ Таблица DV6 не найдена после ожидания")
                rows = []

            dv6_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 5:
                    dv6_data.append({
                        'Дата': cells[0].text.strip(),
                        'Датчик': cells[1].text.strip(),
                        'Стан': cells[2].text.strip(),
                        'Апарат': self.parse_address(cells[4].text.strip())
                    })

            df_dv6 = pd.DataFrame(dv6_data)
            if not df_dv6.empty:
                df_dv6.to_csv('dv6dv.csv', index=False, encoding='utf-8-sig')
                self.send_progress("Этап 2/9", 100, f"✅ DV6: сохранено {len(dv6_data)} записей")
            else:
                self.send_progress("Этап 2/9", 100, "✅ DV6: данных не найдено")

            return True

        except Exception as e:
            self.send_progress("Этап 2/9", 0, f"❌ Ошибка: {str(e)}")
            return False
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    parser = Stage2Parser()
    parser.run_stage()