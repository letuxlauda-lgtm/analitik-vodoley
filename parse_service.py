#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import requests
import time
import re
import json
import subprocess
import sys
import io
import os
from pathlib import Path
from datetime import datetime, timedelta
from math import radians, sin, cos, asin, sqrt
import folium
from folium.features import DivIcon

# ======================== КЛАСС ЛОГГЕРА ========================

class StringLogger:
    """Накапливает логи в строку для сохранения в файл, дублируя в консоль"""
    def __init__(self):
        self.log_capture = io.StringIO()
    
    def log(self, message):
        # Вывод в консоль сервера
        print(message)
        # Запись в буфер памяти
        self.log_capture.write(str(message) + "\n")
    
    def get_content(self):
        return self.log_capture.getvalue()

# ======================== ФУНКЦИОНАЛ (БЕЗ ИЗМЕНЕНИЙ ЛОГИКИ) ========================

def clean_datetime(val):
    if val is None: return None
    s = str(val).strip().replace("*", " ").strip()
    s = re.sub(r"[^0-9:\- ]", "", s).strip()
    if s == "": return None
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for fmt in formats:
        try: return datetime.strptime(s, fmt)
        except: pass
    try: return pd.to_datetime(s, errors="coerce")
    except: return None

def process_service_data(logger):
    """Stage 11"""
    logger.log("🔄 [Stage 11] Обработка сырых данных...")
    if not os.path.exists("service_mes.csv"):
        logger.log("❌ Файл service_mes.csv не найден!")
        return False

    try:
        df = pd.read_csv("service_mes.csv", header=None)
        # Попытка определить структуру, предполагаем 3 колонки
        if len(df.columns) < 3:
             # Если заголовки есть
             df = pd.read_csv("service_mes.csv")
        
        # Принудительно именуем
        df.columns = ["datetime", "event", "aparat"]
        
        df["datetime"] = df["datetime"].apply(clean_datetime)
        df = df.dropna(subset=["datetime"])
        df["is_on"] = df["event"].astype(str).str.contains("Service ON", na=False)
        df["is_off"] = df["event"].astype(str).str.contains("Service OFF", na=False)
        df["tech"] = df["event"].astype(str).str.extract(r"Service ON - (.+)")
        df = df.sort_values("datetime")
        
        records = []
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
                        "data": start.date(), "aparat": aparat, "start": start.time(),
                        "tech": current_tech, "end": end.time(), "kol-time": kol_time,
                        "v_doroge": 0, "fir_point": "", "last_point": ""
                    })
                    current_on = None
                    current_tech = None
        
        out = pd.DataFrame(records)
        if out.empty:
            logger.log("⚠️ Данные отсутствуют — таблица пуста.")
            return True # Не ошибка, просто пусто
            
        out = out.sort_values(["data", "tech", "start"])
        
        # Расчет времени в дороге
        for tech, group in out.groupby("tech"):
            prev = None
            for idx, row in group.iterrows():
                if prev is not None:
                    t1 = datetime.combine(row["data"], row["start"])
                    t0 = datetime.combine(prev["data"], prev["end"])
                    diff = (t1 - t0).total_seconds() // 60
                    out.loc[idx, "v_doroge"] = int(diff)
                prev = row
                
        # Отметки точек
        out["fir_point"] = ""
        out["last_point"] = ""
        for (tech, day), group in out.groupby(["tech", "data"]):
            group = group.sort_values("start")
            if not group.empty:
                out.loc[group.index[0], "fir_point"] = "YES"
                out.loc[group.index[-1], "last_point"] = "YES"
                
        out.to_csv("service_tex_analitik.csv", index=False, encoding="utf-8-sig")
        logger.log("✅ Stage 11 завершен. Файл: service_tex_analitik.csv")
        return True
    except Exception as e:
        logger.log(f"❌ Ошибка в Stage 11: {e}")
        return False

# --- Функции для карт ---
CACHE_FILE = "address_cache.json"
OUT_HTML = "interactive_routes_map.html"
OUT_SUMMARY = "service_routes_summary.csv"

def load_cache():
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def clean_address(raw):
    if pd.isna(raw): return ""
    s = str(raw)
    s = re.sub(r"Близенько.*", "", s, flags=re.I)
    s = re.sub(r"\b\d+(\.\d+)?\s*грн\b", "", s, flags=re.I)
    s = re.sub(r"магаз(ин)?\b.*", "", s, flags=re.I)
    s = re.sub(r"(АТБ|Сільпо|Рукавичка|Фора|BILLA|FOZZY|VARUS).*", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) >= 2: s = f"{parts[0]}, {parts[1]}"
    elif len(parts) == 1: s = parts[0]
    return s.strip()

def normalize_for_cache(addr):
    a = addr.lower().strip()
    a = re.sub(r"вул\.|вулиця|улица|ул\.|проспект|пр\.|буд\.|будинок", "", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def geocode_nominatim(address, cache, logger, city_hint="Львів"):
    if not address: return None, None
    norm = normalize_for_cache(address)
    if norm in cache:
        v = cache[norm]
        return v.get("lat"), v.get("lon")
    
    q = f"{city_hint} {address}"
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": q, "format": "json", "limit": 1, "addressdetails": 0}
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "WaterRoutesBot/1.0"}, timeout=10)
        data = resp.json()
    except:
        return None, None
        
    if not data:
        cache[norm] = {"lat": None, "lon": None}
        logger.log(f"✖ Не найдено: '{address}'")
        time.sleep(1.0)
        return None, None
        
    lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
    cache[norm] = {"lat": lat, "lon": lon}
    logger.log(f"✓ Геокод: '{address}'")
    time.sleep(1.0)
    return lat, lon

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

def build_routes_map(logger):
    """Stage 11A & 11B"""
    logger.log("\n🗺️  [Stage 11A/B] Построение маршрутов...")
    if not Path("service_tex_analitik.csv").exists():
        logger.log("❗ Файл service_tex_analitik.csv не найден.")
        return False
        
    df = pd.read_csv("service_tex_analitik.csv", dtype=str)
    if df.empty: return True

    df.columns = [c.strip() for c in df.columns]
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    
    def parse_time(s):
        if pd.isna(s): return None
        s = str(s).strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try: return pd.to_datetime(s, format=fmt).time()
            except: pass
        return None
        
    df["start_t"] = df["start"].apply(parse_time)
    df["end_t"] = df["end"].apply(parse_time)
    
    cache = load_cache()
    # Группировка
    try:
        grouped = df.sort_values(["data", "tech", "start_t"]).groupby(["data", "tech"], sort=True)
    except KeyError:
        logger.log("❌ Ошибка структуры CSV файла.")
        return False

    fmap = folium.Map(location=[49.8397, 24.0297], zoom_start=12)
    colors = ["red", "blue", "green", "purple", "orange", "darkred", "cadetblue", "darkblue", "darkgreen"]
    tech_colors = {}
    summary = []
    
    for (date, tech), grp in grouped:
        if pd.isna(date) or str(tech).strip() == "nan": continue
        
        coords, points, aparats = [], [], []
        for _, r in grp.iterrows():
            addr = clean_address(r["aparat"])
            lat, lon = geocode_nominatim(addr, cache, logger)
            if lat is None: continue
            
            coords.append((lat, lon))
            points.append((addr, lat, lon, r["start_t"], r["end_t"]))
            if addr not in aparats: aparats.append(addr)
            
        if not coords: continue
        
        tk = str(tech).strip()
        if tk not in tech_colors:
            tech_colors[tk] = colors[len(tech_colors) % len(colors)]
        col = tech_colors[tk]
        
        folium.PolyLine(coords, color=col, weight=4, opacity=0.8).add_to(fmap)
        
        km = sum(haversine_km(*coords[i], *coords[i+1]) for i in range(len(coords)-1))
        
        for i, (addr, lat, lon, st, en) in enumerate(points, 1):
            popup = f"<b>{i}. {addr}</b><br><b>{tk}</b> — {date}<br>"
            if pd.notna(st): popup += f"⏰ {st} "
            if pd.notna(en): popup += f"→ {en}"
            
            folium.Marker(
                [lat, lon], 
                icon=DivIcon(
                    icon_size=(30,12), icon_anchor=(15,12),
                    html=f'<div style="font-size:12px;font-weight:bold;color:{col}">{i}</div>'
                ), 
                popup=popup
            ).add_to(fmap)
            
        summary.append({"date": str(date), "tech": tk, "aparats": len(aparats), "km": round(km, 3), "points": len(points)})
    
    # Легенда
    if tech_colors:
        leg = '<div style="position:fixed;bottom:50px;left:10px;z-index:9999;background:#fff;padding:8px;border:1px solid #ccc;font-size:12px;"><b>Техники</b><br>'
        for t, c in tech_colors.items():
            leg += f'<div style="margin-top:4px;"><span style="display:inline-block;width:12px;height:12px;background:{c};margin-right:6px;"></span>{t}</div>'
        leg += "</div>"
        fmap.get_root().html.add_child(folium.Element(leg))
        
    save_cache(cache)
    fmap.save(OUT_HTML)
    logger.log(f"✅ Карта создана: {OUT_HTML}")
    
    if summary:
        pd.DataFrame(summary).sort_values(["date", "tech"]).to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    
    return True

def run_process_service_data_script(logger):
    """Запускает process_service_data.py если он есть как отдельный скрипт"""
    logger.log("\n🔧 [Stage 12] Запуск process_service_data.py...")
    
    script_path = Path("process_service_data.py")
    if not script_path.exists():
        logger.log(f"ℹ️ Файл {script_path} не найден, пропускаем этот шаг.")
        return True
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            encoding="utf-8"
        )
        logger.log(result.stdout)
        if result.stderr:
            logger.log(f"Stderr: {result.stderr}")
        return True
    except Exception as e:
        logger.log(f"❌ Ошибка запуска скрипта: {e}")
        return False

# ======================== ГЛАВНАЯ ФУНКЦИЯ ЗАПУСКА ========================

def run_full_cycle(callback=None):
    """
    Запускает полный цикл парсинга сервиса.
    callback(text): функция для обновления статуса в Телеграм.
    """
    logger = StringLogger()
    
    def update_status(msg):
        if callback: callback(msg)
        logger.log(msg)

    try:
        logger.log("="*60)
        logger.log(f"🚀 ЗАПУСК ПАРСИНГА СЕРВИСА")
        logger.log(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.log("="*60)
        
        # Stage 11
        update_status("⏳ [1/3] Обработка таблицы...")
        if not process_service_data(logger):
             update_status("❌ Ошибка на этапе 1")
             return False
        
        # Stage 11A & 11B
        update_status("⏳ [2/3] Построение карты и маршрутов...")
        if not build_routes_map(logger):
             update_status("❌ Ошибка на этапе карты")
             return False
        
        # Stage 12 (Optional script)
        update_status("⏳ [3/3] Финализация...")
        run_process_service_data_script(logger)
        
        update_status("✅ Все этапы успешно завершены!")
        return True
        
    except Exception as e:
        err_msg = f"❌ Критическая ошибка: {e}"
        update_status(err_msg)
        logger.log(err_msg)
        import traceback
        logger.log(traceback.format_exc())
        return False
    
    finally:
        # Сохраняем лог в файл в любом случае
        with open("otchet_service.txt", "w", encoding="utf-8") as f:
            f.write(logger.get_content())

if __name__ == "__main__":
    # Тестовый запуск
    run_full_cycle(callback=print)