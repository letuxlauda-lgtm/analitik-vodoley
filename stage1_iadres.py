import time
import re
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


class Stage1Parser:
    def __init__(self, callback=None):
        self.callback = callback
        self.driver = None
        self.wait = None

    def send_progress(self, stage, progress, message):
        """Отправка прогресса выполнения"""
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

    def safe_select_by_name(self, name, value):
        try:
            sel = self.driver.find_element(By.NAME, name)
            Select(sel).select_by_value(str(value))
            time.sleep(0.2)
            if self.is_fatal_page():
                self.send_progress("Система", 0, "⚠️ Fatal после установки селекта — назад и смена дат")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    try:
                        sel = self.driver.find_element(By.NAME, name)
                        Select(sel).select_by_value(str(value))
                        time.sleep(0.2)
                    except Exception:
                        pass
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry select страница снова Fatal — пропускаем")
                        return False
            return True
        except NoSuchElementException:
            return False
        except Exception as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_select_by_name: {e}")
            return False
    # ----------------- Утилиты парсинга -----------------
    def parse_address(self, address):
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

    # ----------------- Методы сбора DV2 -----------------
    def _collect_dv2_stats(self, df_idadres, column_name, days_ago, time_to_wait):
        try:
            target_date = datetime.now() - timedelta(days=days_ago)

            self.safe_select_by_name('date_month_start', datetime.now().month)
            time.sleep(0.15)
            self.safe_select_by_name('date_month_end', datetime.now().month)
            time.sleep(0.15)

            if not self.safe_select_by_name('date_day_start', target_date.day):
                return df_idadres

            clicked = self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=time_to_wait)
            if not clicked:
                return df_idadres

            time.sleep(time_to_wait)
            return self._process_dv2_data(df_idadres, column_name)
        except Exception:
            return df_idadres

    def _collect_dv2_stats_month(self, df_idadres, column_name, time_to_wait):
        try:
            today = datetime.now()
            last_month = today.month - 1 if today.month > 1 else 12

            if not self.safe_select_by_name('date_day_start', today.day):
                return df_idadres
            time.sleep(0.15)
            self.safe_select_by_name('date_day_end', today.day)
            time.sleep(0.15)
            if not self.safe_select_by_name('date_month_start', last_month):
                return df_idadres
            time.sleep(0.15)

            clicked = self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=time_to_wait)
            if not clicked:
                return df_idadres

            time.sleep(time_to_wait)
            return self._process_dv2_data(df_idadres, column_name)
        except Exception:
            return df_idadres

    def _process_dv2_data(self, df_idadres, column_name):
        try:
            table_xpath = "//table[.//th[contains(text(), 'DV2')]]"
            try:
                table = self.wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
            except TimeoutException:
                return df_idadres
            time.sleep(1.2)

            rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
            dv2_updates = {}

            for row in rows:
                td = row.find_elements(By.TAG_NAME, "td")
                if len(td) >= 6:
                    id_val = td[0].text.strip()
                    dv2_off_val = td[5].text.strip()

                    try:
                        dv2_off_num = float(dv2_off_val.replace(',', '.'))
                        if dv2_off_num != 0:
                            dv2_updates[int(id_val)] = dv2_off_val
                    except ValueError:
                        pass

            for id_val, dv2_val in dv2_updates.items():
                idx = df_idadres.index[df_idadres['id'] == id_val].tolist()
                if idx:
                    df_idadres.loc[idx[0], column_name] = dv2_val

            return df_idadres

        except Exception:
            return df_idadres

    # ----------------- Главный метод -----------------
    def run_stage(self):
        self.send_progress("Этап 1/9", 0, "🔍 Инициализация браузера...")
        
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 15)
        except Exception as e:
            self.send_progress("Этап 1/9", 0, f"❌ Не удалось инициализировать WebDriver: {e}")
            return False

        try:
            self.send_progress("Этап 1/9", 5, "🔐 Авторизация...")
            if not self.safe_get("https://soliton.net.ua/water/baza/"):
                self.send_progress("Этап 1/9", 0, "⚠️ Пропускаю авторизацию из-за Fatal страницы")
                return False

            time.sleep(1)
            try:
                self.driver.find_element(By.NAME, "auth_login").send_keys("Service_zenya")
                self.driver.find_element(By.NAME, "auth_pass").send_keys("zenya")
                submit = self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
                submit.click()
                time.sleep(1.2)
            except Exception as e:
                self.send_progress("Этап 1/9", 0, f"❌ Не удалось заполнить форму авторизации: {e}")
                return False

            auth_marker_xpath = "//a[@href='/water/baza/?fid=2&subsection=stat']"
            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, auth_marker_xpath)))
                self.send_progress("Этап 1/9", 10, "✅ Авторизация успешна")
            except TimeoutException:
                self.send_progress("Этап 1/9", 0, "❌ Не удалось авторизоваться")
                return False

            time.sleep(1)

            self.send_progress("Этап 1/9", 15, "📊 Сбор ID и адресов...")
            table_xpath = "//table[.//th[text()='ID'] and .//th[text()='Адреса']]"
            try:
                table = self.wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
                time.sleep(1.2)
            except TimeoutException:
                self.send_progress("Этап 1/9", 0, "⚠️ Таблица id/adres не найдена — завершаю этап")
                return False

            rows_to_process = table.find_elements(By.XPATH, ".//tr[position()>2]")
            idadres_data = []

            for row in rows_to_process:
                td = row.find_elements(By.TAG_NAME, "td")
                if len(td) >= 2:
                    id_text = td[0].text.strip()
                    address_text = td[1].text.strip()
                    if id_text and any(char.isdigit() for char in id_text):
                        idadres_data.append({
                            'id': id_text,
                            'adress': self.parse_address(address_text)
                        })

            df_idadres = pd.DataFrame(idadres_data)
            if not df_idadres.empty:
                df_idadres['id'] = pd.to_numeric(df_idadres['id'], errors='coerce').fillna(0).astype(int)
                df_idadres['dv2day'] = np.nan
                df_idadres['dv2week'] = np.nan
                df_idadres['dv2moun'] = np.nan
                df_idadres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')
                self.send_progress("Этап 1/9", 20, f"✅ Собрано {len(df_idadres)} аппаратов")
            else:
                self.send_progress("Этап 1/9", 20, "⚠️ Не найдено аппаратов в таблице")
                return True # Продолжаем, чтобы не упасть, но нет смысла в сборе DV2

            self.send_progress("Этап 1/9", 25, "🔗 Переход в статистику...")
            if not self.safe_find_and_click(By.XPATH, auth_marker_xpath, wait_after=1.0):
                self.send_progress("Этап 1/9", 0, "⚠️ Не удалось перейти в статистику — пропускаю дальнейшие сборы DV2")
                return True

            try:
                self.wait.until(EC.url_contains("subsection=stat"))
            except TimeoutException:
                pass
            time.sleep(1.2)

            if not self.safe_find_and_click(By.XPATH, "//a[@href='/water/baza/?fid=2&device_stat=log_general']", wait_after=1.0):
                self.send_progress("Этап 1/9", 0, "⚠️ Не удалось открыть device_stat=log_general — пропускаю DV2")
                return True

            try:
                self.wait.until(EC.url_contains("device_stat=log_general"))
            except TimeoutException:
                pass
            time.sleep(1.0)

            self.send_progress("Этап 1/9", 30, "📅 Сбор данных DV2 за день...")
            df_idadres = self._collect_dv2_stats(df_idadres, 'dv2day', days_ago=1, time_to_wait=13)
            df_idadres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            self.send_progress("Этап 1/9", 50, "📅 Сбор данных DV2 за неделю...")
            df_idadres = self._collect_dv2_stats(df_idadres, 'dv2week', days_ago=7, time_to_wait=12)
            df_idadres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            self.send_progress("Этап 1/9", 70, "📅 Сбор данных DV2 за месяц...")
            df_idadres = self._collect_dv2_stats_month(df_idadres, 'dv2moun', time_to_wait=13)
            df_idadres.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            self.send_progress("Этап 1/9", 100, "✅ Этап 1 завершен")
            return True

        except Exception as e:
            self.send_progress("Этап 1/9", 0, f"❌ Ошибка: {str(e)}")
            return False
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    parser = Stage1Parser()
    parser.run_stage()