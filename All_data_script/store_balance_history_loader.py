import requests
import urllib3
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
urllib3.disable_warnings()
def load_store_balance_history_loader(conn):
    token = get_iiko_token()
    # 📦 URL
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/reports/balance/stores"

    # 📅 Период: с 2022-03-01 до сегодня
    start_date = datetime(2022, 3, 1)
    end_date = datetime.now()

    # 📄 Собираем все дни
    all_data = []

    print("📦 Получение остатков по дням:")
    while start_date <= end_date:
        timestamp = start_date.strftime("%Y-%m-%dT23:59:59")
        params = {
            "key": token,
            "timestamp": timestamp
        }

        try:
            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()
            data = response.json()

            for item in data:
                item["timestamp"] = timestamp
            all_data.extend(data)

            print(f"✅ {start_date.strftime('%Y-%m-%d')} — получено {len(data)} записей")
        except Exception as e:
            print(f"⚠️ {start_date.strftime('%Y-%m-%d')} — ошибка: {e}")

        start_date += timedelta(days=1)

    # 🧾 В DataFrame
    df = pd.DataFrame(all_data)

    if df.empty:
        print("⚠️ Нет данных для загрузки.")
        exit()

    # 🧹 Приведение типов
    df["sum"] = pd.to_numeric(df["sum"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["store"] = df["store"].fillna("")
    df["product"] = df["product"].fillna("")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 🧱 Подключение к SQL Server
    cursor = conn.cursor()

    # 📐 Создание таблицы, если не существует
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sysobjects
        WHERE name = 'stagging_table_iiko_balance_stores' AND xtype = 'U'
    )
    CREATE TABLE stagging_table_iiko_balance_stores (
        storeId NVARCHAR(100),
        productId NVARCHAR(100),
        amount FLOAT,
        sum FLOAT,
        timestamp DATETIME
    )
    """)
    conn.commit()
    cursor.execute("TRUNCATE TABLE stagging_table_iiko_balance_stores")
    conn.commit()

    # 💾 Загрузка в таблицу
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stagging_table_iiko_balance_stores (
                productId, storeId, amount, sum, timestamp
            ) VALUES (?, ?, ?, ?, ?)
        """,
        row["store"], row["product"], row["amount"], row["sum"], row["timestamp"])

    conn.commit()
    cursor.close()

    print("✅ Все данные успешно загружены в stagging_table_iiko_balance_stores")
