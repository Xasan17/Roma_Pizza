import requests
import urllib3
import pandas as pd
import pyodbc
from datetime import datetime

# 🔇 Отключить предупреждения SSL
urllib3.disable_warnings()

# 🔐 Токен
def load_balance_stores(token,conn):
    timestamp = datetime.now().isoformat(timespec='seconds')
    today_date = datetime.now().date()

    # 📦 URL и параметры запроса
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/reports/balance/stores"
    params = {
        "key": token,
        "timestamp": timestamp
    }

    # 📥 Запрос
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            # 📊 Преобразуем JSON в DataFrame
            data = response.json()
            df = pd.json_normalize(data, sep='_')

            # 🧼 Добавим дату как колонку
            df['timestamp'] = today_date

            # 📍 Подключение к SQL Server
            cursor = conn.cursor()

            # 📐 Создание таблицы, если не существует
            cursor.execute("""
                IF OBJECT_ID('dbo.stagging_table_iiko_balance_stores', 'U') IS NULL
                BEGIN
                    CREATE TABLE dbo.stagging_table_iiko_balance_stores (
                        productId UNIQUEIDENTIFIER,
                        storeId UNIQUEIDENTIFIER,
                        amount FLOAT,
                        sum FLOAT,
                        timestamp DATE
                    )
                END
            """)
            conn.commit()

            # 🧹 Удалить записи только за сегодня
            cursor.execute("""
                DELETE FROM dbo.stagging_table_iiko_balance_stores
                WHERE timestamp = ?
            """, today_date)
            conn.commit()

            # 💾 Вставка новых записей
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dbo.stagging_table_iiko_balance_stores (
                        productId, storeId, amount, sum, timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                """,
                    row.get("product"),
                    row.get("store"),
                    row.get("amount"),
                    row.get("sum"),
                    row.get("timestamp")
                )

            conn.commit()
            cursor.close()

            print(f"✅ Загружено {len(df)} записей за {today_date}")

        except Exception as e:
            print("❌ Ошибка при обработке данных:", e)

    else:
        print(f"[!] Ошибка {response.status_code}: {response.text}")
