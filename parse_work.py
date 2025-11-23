import time
import sys

# Попытка импорта всех классов парсинга
try:
    from stage1_iadres import Stage1Parser
    from stage2_dv3dv6 import Stage2Parser
    from stage3_water_api import Stage3Api
    from stage4_dv1dv3_status import Stage4Processor
    from stage5_sorterdv6 import Stage5Processor
    from stage6_service_tex import Stage6Parser
    from stage7_service_analytics import Stage7Analyzer
    from stage8_water_filter_speed import Stage8Processor
    from stage9_add_tds_data import Stage9Processor
except ImportError as e:
    print(f"❌ Ошибка импорта модулей: {e}")
    # Не выходим, чтобы бот не падал, если файла нет, просто выведем ошибку
    pass

# Настройка визуального стиля
BAR_LENGTH = 20
FILLED_CHAR = '🟩'
EMPTY_CHAR = '⬜'

def generate_progress_bar(percent):
    """Генерирует строку вида [🟩🟩🟩⬜⬜] 60%"""
    filled_length = int(BAR_LENGTH * percent // 100)
    bar = FILLED_CHAR * filled_length + EMPTY_CHAR * (BAR_LENGTH - filled_length)
    return f"[{bar}] {int(percent)}%"

def console_callback(text):
    """Вывод в консоль"""
    print(f"\r{text}")

# ВАЖНО: Аргумент должен называться именно 'callback'
def run_full_cycle(callback=None):
    """
    Запускает полный цикл парсинга.
    :param callback: Функция, принимающая строку (для отправки в Telegram)
    """
    
    # Если callback не передан, используем вывод в консоль
    if callback is None:
        callback = console_callback

    # Список этапов: (Класс, Описание)
    stages = [
        (Stage1Parser, "Stage 1: iadres"),
        (Stage2Parser, "Stage 2: DV3/DV6"),
        (Stage3Api, "Stage 3: Water API"),
        (Stage4Processor, "Stage 4: Status"),
        (Stage5Processor, "Stage 5: Sort DV6"),
        (Stage6Parser, "Stage 6: Service"),
        (Stage7Analyzer, "Stage 7: Analytics"),
        (Stage8Processor, "Stage 8: Filters"),
        (Stage9Processor, "Stage 9: TDS Data")
    ]

    total_stages = len(stages)
    start_time = time.time()

    callback(f"🚀 Старт парсинга (Всего этапов: {total_stages})")

    for i, (StageClass, stage_name) in enumerate(stages):
        current_stage_num = i + 1
        
        # Функция-обертка для расчета общего прогресса
        def internal_callback(stage_label, stage_local_progress, message):
            # Считаем общий процент: (прошлые этапы + текущий%) / всего
            global_percent = ((i * 100) + stage_local_progress) / total_stages
            if global_percent > 100: global_percent = 100
            
            bar = generate_progress_bar(global_percent)
            
            final_msg = (
                f"🚀 Этап {current_stage_num}/{total_stages}: {stage_name}\n"
                f"{bar}\n"
                f"📝 {message}"
            )
            # Отправляем сообщение через callback (в телеграм)
            callback(final_msg)

        try:
            # Инициализация и запуск этапа
            processor = StageClass(callback=internal_callback)
            result = processor.run_stage()

            if result is False:
                callback(f"⛔️ Остановка: Ошибка на этапе {stage_name}")
                return False
            
            time.sleep(1) 

        except Exception as e:
            callback(f"🔥 КРИТИЧЕСКАЯ ОШИБКА: {stage_name}\n{e}")
            return False

    total_minutes = round((time.time() - start_time) / 60, 1)
    callback(f"🏁 ПАРСИНГ ЗАВЕРШЕН!\n{generate_progress_bar(100)}\nВремя: {total_minutes} мин.")
    return True

if __name__ == "__main__":
    print("Запуск рабочего парсера...")
    run_full_cycle(lambda msg: print(f"\n{msg}"))