import pandas as pd
import re
from datetime import datetime, timedelta


# -------------------- ФУНКЦИЯ ОЧИСТКИ ДАТЫ --------------------
def clean_datetime(val):
    if val is None:
        return None

    s = str(val).strip()

    # Убираем звездочки, мусор
    s = s.replace("*", " ").strip()

    # Оставляем цифры, пробелы, тире и двоеточия
    s = re.sub(r"[^0-9:\- ]", "", s).strip()

    if s == "":
        return None

    # Возможные форматы
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            pass

    # Если pandas сможет — пусть попробует
    try:
        return pd.to_datetime(s, errors="coerce")
    except:
        return None


# -------------------- ОСНОВНАЯ ФУНКЦИЯ --------------------
def process_service_data():

    print("🔄 Обробка даних...")

    df = pd.read_csv("service_mes.csv")

    # Нормализуем названия
    df.columns = ["datetime", "event", "aparat"]

    # Чистим дату
    df["datetime"] = df["datetime"].apply(clean_datetime)

    # Убираем мусорные строки
    df = df.dropna(subset=["datetime"])

    # Флаги событий
    df["is_on"] = df["event"].str.contains("Service ON")
    df["is_off"] = df["event"].str.contains("Service OFF")
    df["tech"] = df["event"].str.extract(r"Service ON - (.+)")

    # Сортировка
    df = df.sort_values("datetime")

    # Результаты
    records = []

    # Группировка по аппарату
    for aparat, group in df.groupby("aparat"):

        group = group.sort_values("datetime")

        current_on = None
        current_tech = None

        for _, row in group.iterrows():
            if row["is_on"]:
                current_on = row["datetime"]
                current_tech = row["tech"]

            elif row["is_off"] and current_on is not None:

                start = current_on
                end = row["datetime"]

                kol_time = int((end - start).total_seconds() // 60)

                records.append({
                    "data": start.date(),
                    "aparat": aparat,
                    "start": start.time(),
                    "tech": current_tech,
                    "end": end.time(),
                    "kol-time": kol_time,
                    "v_doroge": 0,   # заполним позже
                    "fir_point": "",
                    "last_point": ""
                })

                current_on = None
                current_tech = None

    # Переводим в DataFrame
    out = pd.DataFrame(records)

    if out.empty:
        print("⚠️ Дані відсутні — нічого не згенеровано.")
        return []

    # -------------------- РАСЧЁТ V_DOROGE --------------------
    out = out.sort_values(["data", "tech", "start"])

    out["v_doroge"] = 0

    for tech, group in out.groupby("tech"):
        prev = None
        for idx, row in group.iterrows():
            if prev is not None:
                t1 = datetime.combine(row["data"], row["start"])
                t0 = datetime.combine(prev["data"], prev["end"])
                diff = (t1 - t0).total_seconds() // 60
                out.loc[idx, "v_doroge"] = int(diff)
            prev = row

    # -------------------- ПЕРВЫЙ И ПОСЛЕДНИЙ АППАРАТ ЗА ДЕНЬ --------------------
    out["fir_point"] = ""
    out["last_point"] = ""

    for (tech, day), group in out.groupby(["tech", "data"]):
        group = group.sort_values("start")
        out.loc[group.index[0], "fir_point"] = "YES"
        out.loc[group.index[-1], "last_point"] = "YES"

    # -------------------- СОХРАНЕНИЕ --------------------
    out.to_csv("service_tex_analitik.csv", index=False, encoding="utf-8-sig")

    print("✅ Готово! Файл: service_tex_analitik.csv")

    return records


# -------------------- ЗАПУСК --------------------
if __name__ == "__main__":
    process_service_data()
