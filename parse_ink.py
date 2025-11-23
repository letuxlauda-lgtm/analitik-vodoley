import sys
import os
import time

# Пытаемся импортировать этапы
try:
    import stage10_ink
    import stage10a_ink
    import stage10b_ink
except ImportError as e:
    print(f"❌ ОШИБКА ИМПОРТА в parse_ink: {e}")

def run_full_cycle(callback=None):
    """
    Запускает полный цикл: Сбор -> Привязка -> Отчет.
    callback: функция, которая принимает строку (для логов в Telegram).
    """
    
    # Вспомогательная функция для отправки логов
    def log(message):
        print(message) # Пишем в консоль сервера
        if callback:
            callback(message) # Отправляем в Telegram (изменяем сообщение)

    # Адаптер для callback-а из Stage 10 (там 3 аргумента, а нам нужен 1 строка)
    def stage10_adapter(stage, progress, message):
        log(f"[{stage}] {progress}%: {message}")

    log("🚀 ЗАПУСК ЦИКЛА ИНКАСАЦИЙ...")

    # --- ЭТАП 1: Stage 10 (Сбор данных с API) ---
    log("📡 Stage 10: Старт сбора данных...")
    try:
        # Инициализируем процессор, передаем адаптер
        processor = stage10_ink.Stage10InkasProcessor(callback=stage10_adapter)
        success_stage10 = processor.run_stage()
        
        if not success_stage10:
            log("❌ ОСТАНОВКА: Ошибка на этапе сбора данных (Stage 10).")
            return False
    except Exception as e:
        log(f"❌ Критическая ошибка Stage 10: {e}")
        return False

    # --- ЭТАП 2: Stage 10a (Обработка и привязка техников) ---
    log("⚙️ Stage 10a: Обработка и привязка техников...")
    try:
        # process_inkas_data обычно просто принтит, поэтому мы просто ждем выполнения
        success_stage10a = stage10a_ink.process_inkas_data()
        
        if success_stage10a:
            log("✅ Stage 10a: Привязка завершена успешно.")
        else:
            log("❌ ОСТАНОВКА: Ошибка обработки данных (Stage 10a).")
            return False
    except Exception as e:
        log(f"❌ Критическая ошибка Stage 10a: {e}")
        return False

    # --- ЭТАП 3: Stage 10b (Генерация отчета) ---
    log("📄 Stage 10b: Генерация отчета...")
    try:
        # Генерируем файл
        result_msg = stage10b_ink.create_inkas_report()
        
        if "Ошибка" in result_msg:
             log(f"❌ Ошибка генерации отчета: {result_msg}")
             return False
        
        log(f"✅ Файл отчета создан: {result_msg}")

    except Exception as e:
        log(f"❌ Критическая ошибка Stage 10b: {e}")
        return False

    log("🎉 ВСЕ ЭТАПЫ УСПЕШНО ЗАВЕРШЕНЫ!")
    return True

def get_final_report_text():
    """
    Возвращает текст краткого отчета для отправки в чат.
    Использует функцию из stage10b_ink.
    """
    try:
        return stage10b_ink.get_short_report()
    except Exception as e:
        return f"Не удалось получить текст отчета: {e}"

if __name__ == "__main__":
    # Для теста запускаем без колбэка
    run_full_cycle()