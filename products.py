import requests
import pandas as pd
import urllib3
import pyodbc

# Отключаем предупреждения об SSL
urllib3.disable_warnings()

def load_products(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/products/list"
    params = {
        "key": token,
        "includeDeleted": "false"
    }

    # Запрос к API
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            # Преобразование JSON в DataFrame с раскрытием вложенных объектов
            data = response.json()
            df = pd.json_normalize(data, sep='_')
            # Подключение к SQL Server
            cursor = conn.cursor()

            # Пересоздаём таблицу: удаляем, если уже есть
            cursor.execute("""
                IF OBJECT_ID('dbo.stagging_table_iiko_products', 'U') IS NOT NULL
                    DROP TABLE dbo.stagging_table_iiko_products
            """)
            conn.commit()

            # Создаём таблицу заново
            cursor.execute("""
                CREATE TABLE dbo.stagging_table_iiko_products (
                    id UNIQUEIDENTIFIER PRIMARY KEY,
                    code NVARCHAR(255),
                    name NVARCHAR(255),
                    type NVARCHAR(100),
                    num NVARCHAR(100),
                    parent_id UNIQUEIDENTIFIER,
                    category_id UNIQUEIDENTIFIER
                )
            """)
            conn.commit()

            # Вставляем данные в таблицу
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dbo.stagging_table_iiko_products 
                    (id, code, name, type, num, parent_id, category_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    row.get("id"),
                    row.get("code"),
                    row.get("name"),
                    row.get("type"),
                    row.get("num"),
                    row.get("parent"),
                    row.get("category")
                )

            # Завершаем транзакцию и закрываем соединение
            conn.commit()
            cursor.close()

            print("✅ Номенклатура успешно загружена в SQL Server")

        except Exception as e:
            print("❌ Ошибка при разборе JSON:", e)

    else:
        print(f"[!] Ошибка {response.status_code}: {response.text}")
