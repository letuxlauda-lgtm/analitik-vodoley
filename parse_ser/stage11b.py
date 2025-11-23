#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_routes_map_haversine.py

- Читает service_tex_analitik.csv
- Геокодит адреса через Nominatim (с кешем address_cache.json)
- Строит маршрут по дням и по техникам (в порядке времени start)
- Считает расстояния по прямой (haversine)
- Сохраняет interactive_routes_map.html и service_routes_summary.csv
"""

import pandas as pd
import requests
import time
import re
import json
from pathlib import Path
from math import radians, sin, cos, asin, sqrt
import folium
from folium.features import DivIcon

# --- настройки ---
CACHE_FILE = "address_cache.json"
OUT_HTML = "interactive_routes_map.html"
OUT_SUMMARY = "service_routes_summary.csv"
NOMINATIM_SLEEP = 1.0  # секунда между запросами к Nominatim

# --- утилиты ---

def load_cache():
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def clean_address(raw):
    """
    Убирает лишние торговые подписи/цены и оставляет 'Улица, номер' максимально чисто.
    """
    if pd.isna(raw):
        return ""
    s = str(raw)

    # убрать явные "Близенько 1.50 грн" и прочий рекламный мусор
    s = re.sub(r"Близенько.*", "", s, flags=re.I)
    s = re.sub(r"\b\d+(\.\d+)?\s*грн\b", "", s, flags=re.I)
    s = re.sub(r"магаз(ин)?\b.*", "", s, flags=re.I)
    s = re.sub(r"(АТБ|Сільпо|Рукавичка|Фора|BILLA|FOZZY|VARUS).*", "", s, flags=re.I)

    # если после запятой идут лишние слова — оставим только часть до первой группы слов после номера
    # сначала упростим пробелы
    s = re.sub(r"\s+", " ", s).strip()

    # если есть "улица/вул./ул." и т.п. — сохраним, но упростим
    # оставим всё до второго запятой, или до конца номера дома
    # попытка убрать лишние фразы в конце: оставим только "ул ..., <номер>"
    # общая простая евристика: если есть запятая — оставляем до первого слова, после которого идёт номер
    # проще: удаляем всё, что содержит слова 'Близенько' или 'грн' (выше) и затем оставляем до конца номера
    # Оставим только первые две части, разделённые запятой (улица, дом)
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    if len(parts) >= 2:
        s = f"{parts[0]}, {parts[1]}"
    elif len(parts) == 1:
        s = parts[0]
    else:
        s = s

    s = s.strip()
    return s

def normalize_for_cache(addr):
    a = addr.lower().strip()
    a = re.sub(r"вул\.|вулиця|улица|ул\.|проспект|пр\.|буд\.|будинок", "", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a

def geocode_nominatim(address, cache, city_hint="Львів"):
    """
    Возвращает (lat, lon) или (None, None). Использует cache (мутирует).
    """
    if not address:
        return None, None

    norm = normalize_for_cache(address)
    if norm in cache:
        v = cache[norm]
        return v.get("lat"), v.get("lon")

    q = f"{city_hint} {address}"
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": q, "format": "json", "limit": 1, "addressdetails": 0}

    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "WaterRoutesBot/1.0 (+https://example.com)"}, timeout=15)
        data = resp.json()
    except Exception:
        # временный провал — пометим None чтобы не пытаться бесконечно
        cache[norm] = {"lat": None, "lon": None}
        return None, None

    if not data:
        cache[norm] = {"lat": None, "lon": None}
        print(f"✖ Не знайдено: '{address}' (запрос: '{q}')")
        time.sleep(NOMINATIM_SLEEP)
        return None, None

    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    cache[norm] = {"lat": lat, "lon": lon}
    print(f"✓ Геокод: '{address}' → {lat:.6f}, {lon:.6f}")
    time.sleep(NOMINATIM_SLEEP)
    return lat, lon

def haversine_km(lat1, lon1, lat2, lon2):
    # все в радианах
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c

# --- главная логика ---

def build_routes_map(hide_missing_addresses=False):
    print("Старт: строим маршруты (Haversine).")

    # загрузка файла
    if not Path("service_tex_analitik.csv").exists():
        print("Файл service_tex_analitik.csv не найден в текущей папке.")
        return

    df = pd.read_csv("service_tex_analitik.csv", dtype=str)  # читаем как строки, чтобы быть устойчивыми
    if df.empty:
        print("service_tex_analitik.csv пуст.")
        return

    # ожидаемые колонки: data, aparat, start, tech, end, kol-time, v_doroge, ...
    # нормализуем названия колонок (на случай других кодировок)
    df.columns = [c.strip() for c in df.columns]

    # гарантируем, что колонки присутствуют
    for c in ["data", "aparat", "start", "tech", "end"]:
        if c not in df.columns:
            print(f"❗ Входной CSV не содержит колонку '{c}'. Исправь имя колонки или переименуй.")
            return

    # парсим data как date, start/end как время (если возможно)
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    # Try to parse start/end as times
    def parse_time(s):
        if pd.isna(s):
            return None
        s = str(s).strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return pd.to_datetime(s, format=fmt).time()
            except Exception:
                pass
        # fallback: try pandas general parse
        try:
            return pd.to_datetime(s, errors="coerce").time()
        except Exception:
            return None

    df["start_t"] = df["start"].apply(parse_time)
    df["end_t"] = df["end"].apply(parse_time)

    # загружаем кеш
    cache = load_cache()

    # сгруппируем по дате и техник
    grouped = df.sort_values(["data", "tech", "start_t"]).groupby(["data", "tech"], sort=True)

    # Создадим карту
    center = [49.8397, 24.0297]
    fmap = folium.Map(location=center, zoom_start=12)

    # цвета для техников
    base_colors = ["red", "blue", "green", "purple", "orange", "darkred", "cadetblue", "darkblue", "darkgreen"]
    tech_color_map = {}
    summary_rows = []

    # проходим группы
    for (date, tech), group in grouped:
        if pd.isna(date) or (str(tech).strip() == "nan"):
            continue

        grp = group.copy()
        # порядок уже гарантирован сортировкой выше

        # список точек для маршрута (в порядке)
        route_coords = []
        route_points_info = []  # (aparat, lat, lon, start_t, end_t)
        unique_aparats = []
        for _, r in grp.iterrows():
            aparat_raw = r["aparat"]
            aparat_clean = clean_address(aparat_raw)
            # геокод
            lat, lon = geocode_nominatim(aparat_clean, cache)
            if lat is None or lon is None:
                # если не найден — пропускаем точку
                if hide_missing_addresses:
                    continue
                else:
                    # продолжаем, но не включаем в маршрут
                    continue

            route_coords.append((lat, lon))
            route_points_info.append((aparat_clean, lat, lon, r["start_t"], r["end_t"]))

            # учитываем уникальные аппараты (по тексту очищенному)
            if aparat_clean not in unique_aparats:
                unique_aparats.append(aparat_clean)

        if not route_coords:
            # ничего не нашлось для этого (date, tech)
            continue

        # назначаем цвет технику
        tech_key = str(tech).strip()
        if tech_key not in tech_color_map:
            tech_color_map[tech_key] = base_colors[len(tech_color_map) % len(base_colors)]
        color = tech_color_map[tech_key]

        # рисуем линию маршрута (polyline)
        folium.PolyLine(locations=route_coords, color=color, weight=4, opacity=0.8).add_to(fmap)

        # рассчитаем расстояние (haversine) между последовательными точками
        total_km = 0.0
        for i in range(len(route_coords) - 1):
            lat1, lon1 = route_coords[i]
            lat2, lon2 = route_coords[i + 1]
            d = haversine_km(lat1, lon1, lat2, lon2)
            total_km += d

        # поставим нумерованные маркеры (с popup)
        for i, info in enumerate(route_points_info, start=1):
            aparat_clean, lat, lon, st, en = info
            popup_html = f"<b>{i}. {aparat_clean}</b><br><b>{tech_key}</b> — {date}<br>"
            if pd.notna(st):
                popup_html += f"⏰ {st} "
            if pd.notna(en):
                popup_html += f"→ {en}"
            folium.Marker(
                [lat, lon],
                icon=DivIcon(icon_size=(30, 12), icon_anchor=(15, 12),
                             html=f'<div style="font-size:12px; font-weight:bold; color:{color}">{i}</div>'),
                popup=popup_html
            ).add_to(fmap)

        # Сохранение строки сводки
        # суммарное "время в дороге" будем считать как сумму длительностей переходов в минутах (approx = distance / 50km/h*60)
        # Но пользователь просил только сколько км и сколько аппаратов, оставим простые метрики:
        total_aparats = len(unique_aparats)
        summary_rows.append({
            "date": str(date),
            "tech": tech_key,
            "aparats_count": total_aparats,
            "km_haversine": round(total_km, 3),
            "points_count": len(route_points_info)
        })

    # добавим легенду (цвета техников)
    if tech_color_map:
        legend_html = """<div style="
            position: fixed;
            bottom: 50px;
            left: 10px;
            z-index: 9999;
            background-color: white;
            padding: 8px;
            border: 1px solid #ccc;
            box-shadow: 0 0 6px rgba(0,0,0,0.2);
            font-size:12px;
            ">
            <b>Техники / Цвета</b><br>"""
        for t, c in tech_color_map.items():
            legend_html += f'<div style="margin-top:4px;"><span style="display:inline-block;width:12px;height:12px;background:{c};margin-right:6px;"></span>{t}</div>'
        legend_html += "</div>"
        fmap.get_root().html.add_child(folium.Element(legend_html))

    # сохраним кеш и файлы
    save_cache(cache)

    # сохраняем html
    fmap.save(OUT_HTML)
    print(f"Карта сохранена: {OUT_HTML}")

    # сохраняем сводную таблицу
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        summary_df = summary_df.sort_values(["date", "tech"])
        summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
        print(f"Сводный отчёт сохранён: {OUT_SUMMARY}")
    else:
        print("Нет маршрутов для отчёта.")

    print("Готово.")

if __name__ == "__main__":
    build_routes_map()
