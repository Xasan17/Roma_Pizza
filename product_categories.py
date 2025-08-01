import requests
import pandas as pd
import urllib3

urllib3.disable_warnings()
def load_product_categories(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/products/category/list"
    params = {
        "key": token,
        "includeDeleted": "false"
    }

    # 📡 GET-запрос
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        data = response.json()

        # 📄 Преобразование в DataFrame
        df = pd.json_normalize(data)
        # 🧱 Подключение к SQL Server
        cursor = conn.cursor()

        # ❗ Создание таблицы (однократно)
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_product_categories', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_product_categories (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            name NVARCHAR(255),
            deleted BIT
        )
        """)
        conn.commit()

        # 🧹 Очистка таблицы
        cursor.execute("DELETE FROM stagging_table_iiko_product_categories")
        conn.commit()

        # 📤 Вставка данных
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stagging_table_iiko_product_categories (id, name, deleted)
                VALUES (?, ?, ?)
            """, row.get("id"),
                row.get("name"),
                row.get("deleted"))
        conn.commit()

        cursor.close()
        print("✅ Категории успешно загружены в SQL Server")

    else:
        print("[!] Ошибка:", response.status_code, response.text)
