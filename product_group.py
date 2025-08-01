import requests
import pandas as pd
import urllib3
import pyodbc

urllib3.disable_warnings()
def load_prduct_group(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/products/group/list"
    params = {
        "key": token,
        "includeDeleted": "false"
    }

    # 📡 Отправляем GET-запрос
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        data = response.json()

        # 📄 Преобразуем в DataFrame
        df = pd.json_normalize(data)

        # 🛠 Подключение к SQL Server
        cursor = conn.cursor()

        # ❗ Создание таблицы (однократно)
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_product_groups', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_product_groups (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            name NVARCHAR(255),
            description NVARCHAR(255),
            code NVARCHAR(100),
            num NVARCHAR(100),
            parent UNIQUEIDENTIFIER,
            deleted BIT
        )
        """)
        conn.commit()

        # 🧹 Очистка перед вставкой
        cursor.execute("DELETE FROM stagging_table_iiko_product_groups")
        conn.commit()

        # 📤 Вставка в БД
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stagging_table_iiko_product_groups (id, name, description, code, num, parent, deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row.get("id"),
                row.get("name"),
                row.get("description"),
                row.get("code"),
                row.get("num"),
                row.get("parent"),
                row.get("deleted"))
        conn.commit()

        cursor.close()
    else:
        print("[!] Ошибка:", response.status_code, response.text)
