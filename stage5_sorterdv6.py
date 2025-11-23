import os
import pandas as pd
import re
from datetime import datetime


class Stage5Processor:
    def __init__(self, callback=None):
        self.callback = callback

    def send_progress(self, stage, progress, message):
        if self.callback:
            self.callback(stage, progress, message)
        print(f"[{stage}] {progress}% - {message}")

    # ----------------- Главный метод -----------------
    def run_stage(self):
        """Обработка данных DV6"""
        try:
            self.send_progress("Этап 5/9", 0, "📂 Чтение dv6dv.csv...")

            if not os.path.exists('dv6dv.csv'):
                self.send_progress("Этап 5/9", 0, "❌ Файл dv6dv.csv не найден")
                return False
            
            if not os.path.exists('idadres.csv'):
                self.send_progress("Этап 5/9", 0, "❌ Файл idadres.csv не найден")
                return False

            df = pd.read_csv('dv6dv.csv', sep=',', encoding='utf-8-sig')
            
            # Переименование колонок
            df = df.rename(columns={
                'Дата': 'timestamp',
                'Датчик': 'device',
                'Стан': 'action',
                'Апарат': 'address'
            })

            self.send_progress("Этап 5/9", 10, "🔄 Обработка данных DV6...")

            df = df.dropna(subset=['timestamp', 'device', 'action', 'address'], how='all')

            for col in ['timestamp', 'device', 'action', 'address']:
                if col in df.columns and df[col].dtype == 'object':
                    # Очистка от кавычек и лишних пробелов
                    df[col] = df[col].astype(str).str.strip().str.replace('"', '').str.replace("'", '')

            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp'])
            df = df.sort_values(['address', 'timestamp'])

            self.send_progress("Этап 5/9", 30, "🔍 Поиск пар ON/OFF...")

            results = []
            current_on = {}

            for _, row in df.iterrows():
                address = row['address']
                action = str(row['action']).lower()
                timestamp = row['timestamp']

                if action == 'on':
                    current_on[address] = timestamp
                elif action == 'off' and address in current_on:
                    on_time = current_on.pop(address)
                    duration = (timestamp - on_time).total_seconds()
                    results.append({
                        'address': address,
                        'on_time': on_time,
                        'off_time': timestamp,
                        'duration': duration
                    })

            result_df = pd.DataFrame(results)

            self.send_progress("Этап 5/9", 50, "📊 Формирование статистики...")

            def format_long_operations(group):
                # Фильтруем операции дольше 10 минут (600 секунд)
                long_ops = group[group['duration'] > 600] 
                if long_ops.empty:
                    return ''
                lines = []
                for _, op in long_ops.iterrows():
                    on_str = op['on_time'].strftime('%Y-%m-%d %H:%M:%S')
                    off_str = op['off_time'].strftime('%Y-%m-%d %H:%M:%S')
                    lines.append(f"{int(op['duration'])}сек({on_str}-{off_str})")
                return '; '.join(lines)

            if not result_df.empty:
                # Группируем по адресу и считаем количество (dv6raz) и форматируем время (dv6time)
                summary = result_df.groupby('address').apply(
                    lambda x: pd.Series({
                        'dv6raz': len(x),
                        'dv6time': format_long_operations(x)
                    })
                ).reset_index()
                summary['address'] = summary['address'].astype(str)
            else:
                summary = pd.DataFrame(columns=['address', 'dv6raz', 'dv6time'])

            self.send_progress("Этап 5/9", 70, "🔗 Обновление idadres.csv...")

            id_table = pd.read_csv('idadres.csv', sep=',', encoding='utf-8-sig', keep_default_na=False)

            # Поиск столбца с адресом
            address_cols = [col for col in id_table.columns if 'adress' in col.lower() or 'адрес' in col.lower() or 'address' in col.lower()]
            if not address_cols:
                self.send_progress("Этап 5/9", 0, "❌ Не найден столбец адреса в idadres.csv")
                return False
                
            address_col_name = address_cols[0]
            id_table = id_table.rename(columns={address_col_name: 'address'})

            id_table['address'] = id_table['address'].astype(str).str.strip().str.replace('"', '').str.replace("'", '')

            # Удаление старых колонок перед merge
            id_table = id_table.drop(columns=['dv6raz']) if 'dv6raz' in id_table.columns else id_table
            id_table = id_table.drop(columns=['dv6time']) if 'dv6time' in id_table.columns else id_table
            
            # Объединение
            id_table = id_table.merge(summary, on='address', how='left')
            
            # Заполнение пропущенных значений
            id_table['dv6raz'] = id_table['dv6raz'].fillna(0).astype(int)
            id_table['dv6time'] = id_table['dv6time'].fillna('')
            
            # Переименование столбца адреса обратно, если он был переименован
            if address_col_name != 'address':
                id_table = id_table.rename(columns={'address': address_col_name})

            id_table.to_csv('idadres.csv', index=False, encoding='utf-8-sig')

            self.send_progress("Этап 5/9", 100, f"✅ Обработано {len(summary)} аппаратов DV6")
            return True

        except Exception as e:
            self.send_progress("Этап 5/9", 0, f"❌ Ошибка: {str(e)}")
            return False


if __name__ == "__main__":
    processor = Stage5Processor()
    processor.run_stage()