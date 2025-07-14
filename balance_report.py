import requests
import urllib3
import pandas as pd
import pyodbc
from datetime import datetime

urllib3.disable_warnings()

# 🔐 Токен
token = "8063ebd3-dc30-1412-4660-aff906b8b6cd"

# 📅 Текущая дата и время в ISO формате
timestamp = datetime.now().isoformat(timespec='seconds')

# 📦 URL и параметры запроса
url = "https://roma-pizza-co.iiko.it/resto/api/v2/reports/balance/stores"
params = {
    "key": token,
    "timestamp": timestamp
}

# 📥 Запрос
response = requests.get(url, params=params, verify=False)

# ✅ Обработка ответа
if response.ok:
    try:
        # Преобразуем JSON в DataFrame
        data = response.json()
        df = pd.json_normalize(data, sep='_')
        print(df.columns)
        print("📊 Колонки с вложенными объектами:")
        print(df.columns[df.columns.str.contains('parent|category|store|product')])

        # 🔧 Подключение к SQL Server
        conn = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};'
            'SERVER=TA_GEO_07\\SQLEXPRESS;'
            'DATABASE=Roma_pizza;'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # 🧹 Удаляем таблицу, если есть
        cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_balance', 'U') IS NOT NULL
                DROP TABLE dbo.stagging_table_iiko_balance
        """)
        conn.commit()

        # 🏗️ Создаем таблицу заново
        cursor.execute("""
            CREATE TABLE dbo.stagging_table_iiko_balance (
                productId UNIQUEIDENTIFIER,
                storeId UNIQUEIDENTIFIER,
                amount FLOAT,
                sum FLOAT
            )
        """)
        conn.commit()

        # 💾 Вставка данных в таблицу
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dbo.stagging_table_iiko_balance (
                    productId, storeId, amount, sum
                ) VALUES (?, ?, ?, ?)
            """,
                row.get("product"),
                row.get("store"),
                row.get("amount"),
                row.get("sum")
            )

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Остатки успешно загружены в SQL Server")

    except Exception as e:
        print("❌ Ошибка при обработке данных:", e)

else:
    print(f"[!] Ошибка {response.status_code}: {response.text}")
