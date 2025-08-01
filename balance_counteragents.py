import requests
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime

# 🔇 Отключить предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def load_balance_contragents(token, conn):
    base_url = "https://roma-pizza-co.iiko.it/resto"
    url = f"{base_url}/api/v2/reports/balance/counteragents"

    # 📅 Текущая дата
    today = datetime.now().date()
    today_ts = datetime.now().strftime("%Y-%m-%dT23:59:59")

    # 📥 Запрос
    params = {
        "key": token,
        "timestamp": today_ts
    }

    try:
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[!] Ошибка запроса: {e}")
        exit()

    # 📄 В DataFrame
    df = pd.DataFrame(data)
    df["sum"] = pd.to_numeric(df["sum"], errors="coerce")
    df["counteragent"] = df["counteragent"].fillna("")
    df["timestamp"] = today_ts

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
    try:
        delete_sql = """
            DELETE FROM stagging_table_iiko_balance_counteragents
            WHERE CAST(timestamp AS DATE) = ?
        """
        cursor.execute(delete_sql, (today,))
        conn.commit()
    except Exception as e:
        print("Ошибка при удалении записей:", e)

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
    print("✅ Данные за сегодня успешно загружены в SQL Server")
