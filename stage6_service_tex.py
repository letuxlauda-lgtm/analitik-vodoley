import time
import os
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


class Stage6Parser:
    def __init__(self, callback=None):
        self.callback = callback
        self.driver = None
        self.wait = None

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")
        
    # ----------------- Утилиты для работы с Selenium и Fatal Error (сокращены) -----------------
    def is_fatal_page(self):
        try:
            html = self.driver.page_source.lower()
            return "fatal error" in html or "allowed memory size" in html or "memory size" in html
        except Exception:
            return False
            
    def set_today_dates_on_page(self):
        today = datetime.now()
        day = str(today.day)
        month = str(today.month)
        year = str(today.year)

        names = [('date_day_start', day), ('date_month_start', month), ('date_year_start', year),
                 ('date_day_end', day), ('date_month_end', month), ('date_year_end', year)]
        changed = False
        for name, value in names:
            try:
                Select(self.driver.find_element(By.NAME, name)).select_by_value(value)
                changed = True
                time.sleep(0.15)
            except (NoSuchElementException, Exception):
                continue
        return changed

    def try_back_and_fix_dates(self):
        try:
            self.driver.back() if 'history' not in self.driver.current_url else self.driver.execute_script('window.history.back()')
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
                self.send_progress("Система", 0, "⚠️ Fatal при get — Back и даты")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    self.driver.get(url); time.sleep(wait_seconds)
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry снова Fatal — пропускаем")
                        return False
            return True
        except (WebDriverException, Exception) as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_get: {e}")
            return False

    def safe_find_and_click(self, by, value, wait_after=1.0):
        try:
            elem = self.wait.until(EC.presence_of_element_located((by, value)))
            elem.click()
            time.sleep(wait_after)
            if self.is_fatal_page():
                self.send_progress("Система", 0, "⚠️ Fatal после click — Back и даты")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    elem = self.wait.until(EC.presence_of_element_located((by, value)))
                    elem.click()
                    time.sleep(wait_after)
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry снова Fatal — пропускаем")
                        return False
            return True
        except (TimeoutException, WebDriverException, Exception) as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_find_and_click: {e}")
            return False

    def safe_select_by_name(self, name, value):
        try:
            sel = self.driver.find_element(By.NAME, name)
            Select(sel).select_by_value(str(value))
            time.sleep(0.2)
            if self.is_fatal_page():
                self.send_progress("Система", 0, "⚠️ Fatal после select — Back и даты")
                ok = self.try_back_and_fix_dates()
                if not ok:
                    Select(self.driver.find_element(By.NAME, name)).select_by_value(str(value)); time.sleep(0.2)
                    if self.is_fatal_page():
                        self.send_progress("Система", 0, "❌ После retry снова Fatal — пропускаем")
                        return False
            return True
        except NoSuchElementException:
            return False
        except Exception as e:
            self.send_progress("Система", 0, f"❌ Ошибка в safe_select_by_name: {e}")
            return False
            
    # ----------------- Главный метод -----------------
    def run_stage(self):
        self.send_progress("Этап 6/9", 0, "🔍 Инициализация браузера...")
        
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
            self.send_progress("Этап 6/9", 0, f"❌ Не удалось инициализировать WebDriver: {e}")
            return False

        try:
            self.send_progress("Этап 6/9", 5, "🔐 Авторизация...")
            # Повторная авторизация
            if not self.safe_get("https://soliton.net.ua/water/baza/"):
                self.send_progress("Этап 6/9", 0, "⚠️ Пропускаю авторизацию из-за Fatal страницы")
                return False

            time.sleep(1)
            try:
                self.driver.find_element(By.NAME, "auth_login").send_keys("Service_zenya")
                self.driver.find_element(By.NAME, "auth_pass").send_keys("zenya")
                self.driver.find_element(By.CSS_SELECTOR, "input[type='submit']").click()
                time.sleep(1.2)
                self.wait.until(EC.presence_of_element_located((By.XPATH, "//a[@href='/water/baza/?fid=2&subsection=stat']")))
            except Exception as e:
                self.send_progress("Этап 6/9", 0, f"❌ Не удалось авторизоваться: {e}")
                return False

            self.send_progress("Этап 6/9", 10, "🔗 Переход в раздел датчиков...")
            if not self.safe_find_and_click(By.XPATH, "//a[@href='/water/baza/?section=sensors&fid=2']", wait_after=1.2):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось перейти в раздел sensors — пропускаю")
                return True
            time.sleep(1.2)

            self.send_progress("Этап 6/9", 15, "🔗 Переход в систему...")
            if not self.safe_find_and_click(By.XPATH, "//a[@href='/water/baza/?fid=2&sensors_stat=system']", wait_after=1.2):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось перейти в system — пропускаю")
                return True
            time.sleep(1.2)

            self.send_progress("Этап 6/9", 20, "📝 Выбор Service...")
            if not self.safe_select_by_name('system', 'Service'):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось выбрать Service — пропускаю")
                return True
            time.sleep(0.4)

            # --- Сбор за день (вчера) ---
            yesterday = datetime.now() - timedelta(days=1)
            if not self.safe_select_by_name('date_day_start', yesterday.day):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось установить дату (день) — пропускаю день")
            time.sleep(0.3)

            self.send_progress("Этап 6/9", 30, "🔄 Запрос данных за день...")
            if not self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=5):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось получить Service день — пропускаю")
                return True
            time.sleep(1.2)

            self.send_progress("Этап 6/9", 40, "📊 Парсинг Service за день...")
            service_day_data = []
            try:
                table = self.driver.find_element(By.XPATH, "//table[.//th[text()='Дата'] and .//th[text()='Подія'] and .//th[text()='Апарат']]")
                rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 3:
                        service_day_data.append({
                            'Дата': cells[0].text.strip(),
                            'Подія': cells[1].text.strip(),
                            'Апарат': cells[2].text.strip()
                        })
            except Exception:
                pass

            df_day = pd.DataFrame(service_day_data)
            if not df_day.empty:
                df_day.to_csv('service_day.csv', index=False, encoding='utf-8-sig')
                self.send_progress("Этап 6/9", 60, f"✅ Service день: {len(df_day)} записей")
            else:
                self.send_progress("Этап 6/9", 60, "✅ Service день: записей нет")

            time.sleep(1.2)

            # --- Сбор за месяц (прошлый) ---
            current_month = datetime.now().month
            last_month = current_month - 1 if current_month > 1 else 12
            if not self.safe_select_by_name('date_month_start', last_month):
                self.send_progress("Этап 6/9", 60, "⚠️ Не удалось установить дату (месяц) — пропускаю месяц")
            time.sleep(0.3)

            self.send_progress("Этап 6/9", 70, "🔄 Запрос данных за месяц...")
            if not self.safe_find_and_click(By.CSS_SELECTOR, "input[type='submit'][value='Вивести']", wait_after=11):
                self.send_progress("Этап 6/9", 0, "⚠️ Не удалось получить Service месяц — пропускаю")
                return True
            time.sleep(1.2)

            service_month_data = []
            try:
                table = self.driver.find_element(By.XPATH, "//table[.//th[text()='Дата'] and .//th[text()='Подія'] and .//th[text()='Апарат']]")
                rows = table.find_elements(By.XPATH, ".//tr[position()>1]")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 3:
                        service_month_data.append({
                            'Дата': cells[0].text.strip(),
                            'Подія': cells[1].text.strip(),
                            'Апарат': cells[2].text.strip()
                        })
            except Exception:
                pass


            df_month = pd.DataFrame(service_month_data)
            if not df_month.empty:
                df_month.to_csv('service_mes.csv', index=False, encoding='utf-8-sig')
                self.send_progress("Этап 6/9", 100, f"✅ Service месяц: {len(df_month)} записей")
            else:
                self.send_progress("Этап 6/9", 100, "✅ Service месяц: записей нет")

            return True

        except Exception as e:
            self.send_progress("Этап 6/9", 0, f"❌ Ошибка: {str(e)}")
            return False
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    parser = Stage6Parser()
    parser.run_stage()