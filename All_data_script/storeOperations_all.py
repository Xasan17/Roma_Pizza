import requests
import pandas as pd
import xml.etree.ElementTree as ET
import urllib3
from datetime import datetime  # ✅ Добавили модуль для обработки дат
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
# 🚫 Отключить предупреждения SSL
urllib3.disable_warnings()
def load_storeOperations_all(conn):
    token = get_iiko_token()
    base_url = "https://roma-pizza-co.iiko.it/resto"
    date_from = "01.01.2022"
    date_to = datetime.now().strftime("%d.%m.%Y")
    # ✅ Подключение к SQL Server
    cursor = conn.cursor()

    # 📤 GET-запрос к API
    store_url = f"{base_url}/api/reports/storeOperations"
    store_params = {
        "key": token,
        "dateFrom": date_from,
        "dateTo": date_to,
        "productDetalization": "false",
        "showCostCorrections": "false"
    }

    response = requests.get(store_url, params=store_params, verify=False)
    if response.ok:
        root = ET.fromstring(response.text)
        data = []

        for item in root.findall(".//storeReportItemDto"):
            # ✅ Обработка даты: преобразуем '26.04.2023' → datetime.date
            date_str = item.findtext("date")
            date_obj = None
            if date_str:
                try:
                    date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
                except ValueError:
                    date_obj = None

            data.append({
                "sum": float(item.findtext("sum") or 0),
                "cost": float(item.findtext("cost") or 0),
                "documentSum": float(item.findtext("documentSum") or 0),
                "date": date_obj,
                "type": item.findtext("type"),
                "incoming": item.findtext("incoming") == "true",
                "documentType": item.findtext("documentType"),
                "documentId": item.findtext("documentId"),
                "documentNum": item.findtext("documentNum"),
                "primaryStore": item.findtext("primaryStore"),
                "secondaryAccount": item.findtext("secondaryAccount"),
                "documentComment": item.findtext("documentComment"),
            })

        df = pd.DataFrame(data)

        # 📌 Создание таблицы (если не существует)
        cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_store_operations', 'U') IS NULL
            CREATE TABLE dbo.stagging_table_iiko_store_operations (
                sum FLOAT,
                cost FLOAT,
                documentSum FLOAT,
                date DATE,
                type NVARCHAR(50),
                incoming BIT,
                documentType NVARCHAR(100),
                documentId UNIQUEIDENTIFIER,
                documentNum NVARCHAR(50),
                primaryStore UNIQUEIDENTIFIER,
                secondaryAccount UNIQUEIDENTIFIER,
                documentComment NVARCHAR(MAX)
            )
        """)
        conn.commit()

        # 🔁 Очистка и вставка
        cursor.execute("DELETE FROM dbo.stagging_table_iiko_store_operations")
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dbo.stagging_table_iiko_store_operations (
                    sum, cost, documentSum, date, type, incoming, documentType,
                    documentId, documentNum, primaryStore, secondaryAccount, documentComment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row["sum"], row["cost"], row["documentSum"], row["date"], row["type"],
                row["incoming"], row["documentType"], row["documentId"], row["documentNum"],
                row["primaryStore"], row["secondaryAccount"], row["documentComment"])
        conn.commit()
        cursor.close()
        print("✅ Store operations report saved to SQL.")
    else:
        print(f"❌ Failed: {response.status_code} — {response.text}")
