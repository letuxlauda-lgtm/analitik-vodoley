import sqlite3

def create_db():
    conn = sqlite3.connect('voda_analitik.db')
    cursor = conn.cursor()

    # Шаблон создания таблицы задач
    table_template = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        num INTEGER PRIMARY KEY AUTOINCREMENT,
        id_terem INTEGER,
        adress TEXT,
        zadaca TEXT,
        texnik TEXT,
        date_time_start DATETIME,
        status TEXT DEFAULT 'activ',
        date_time_finish DATETIME,
        vremyareakcii INTEGER
    )
    """

    # Создаем таблицы для каждого техника
    cursor.execute(table_template.format(table_name="zadaci_rus"))
    cursor.execute(table_template.format(table_name="zadaci_dmu"))
    cursor.execute(table_template.format(table_name="zadaci_igo"))
    cursor.execute(table_template.format(table_name="zadaci_cal")) # Для колцентра
    cursor.execute(table_template.format(table_name="zadaci_texd")) 
    cursor.execute(table_template.format(table_name="zadaci_finan"))

    conn.commit()
    conn.close()
    print("База данных и таблицы успешно созданы.")

if __name__ == "__main__":
    create_db()