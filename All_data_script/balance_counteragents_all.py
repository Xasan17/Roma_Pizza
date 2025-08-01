import requests
import pandas as pd
import urllib3
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token

# 🔇 Отключить предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_balance_counteragents_all(conn):
    token = get_iiko_token()
    base_url = "https://roma-pizza-co.iiko.it/resto"
    url = f"{base_url}/api/v2/reports/balance/counteragents"

    date_from = datetime.strptime("01.01.2022", "%d.%m.%Y")
    date_to = datetime.now()

    # 📊 Сбор данных за весь период
    all_data = []

    current_date = date_from
    while current_date <= date_to:
        ts = current_date.strftime("%Y-%m-%dT23:59:59")
        params = {
            "key": token,
            "timestamp": ts
        }

        try:
            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()
            day_data = response.json()

            for item in day_data:
                item["timestamp"] = ts  # Добавляем дату запроса
                all_data.append(item)

            print(f"[✓] Загружено за: {current_date.strftime('%d.%m.%Y')}")

        except Exception as e:
            print(f"[!] Ошибка на {current_date.strftime('%d.%m.%Y')}: {e}")

        current_date += timedelta(days=1)  # Следующий день

    # 📄 В DataFrame
    df = pd.DataFrame(all_data)

    # 🧹 Приведение типов
    df["sum"] = pd.to_numeric(df["sum"], errors="coerce")
    df["counteragent"] = df["counteragent"].fillna("")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 🧱 Подключение к SQL Server
    cursor = conn.cursor()

    # 📐 Создание таблицы
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sysobjects
        WHERE name = 'stagging_table_iiko_balance_counteragents' AND xtype = 'U'
    )
    CREATE TABLE stagging_table_iiko_balance_counteragents (
        account NVARCHAR(100),
        counteragent NVARCHAR(100),
        department NVARCHAR(100),
        sum DECIMAL(18,2),
        timestamp DATETIME
    )
    """)
    conn.commit()

    # 🧽 Очистка (опционально)
    cursor.execute("DELETE FROM stagging_table_iiko_balance_counteragents")
    conn.commit()

    # 💾 Загрузка
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stagging_table_iiko_balance_counteragents (
                account, counteragent, department, sum, timestamp
            ) VALUES (?, ?, ?, ?, ?)
        """,
        row["account"], row["counteragent"], row["department"],
        row["sum"], row["timestamp"])

    conn.commit()
    cursor.close()

    print("✅ Данные за весь период успешно загружены в SQL Server")
