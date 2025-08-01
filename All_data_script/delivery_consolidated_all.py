import requests
import pandas as pd
import xml.etree.ElementTree as ET
import urllib3
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
# 🚫 Отключить предупреждения SSL
urllib3.disable_warnings()
def load_delivery_consolidated_all(conn):
    token = get_iiko_token()
    # 🔑 Настройки подключения
    base_url = "https://roma-pizza-co.iiko.it/resto"
    date_from = "01.01.2022"
    date_to = datetime.now().strftime("%d.%m.%Y")
    # 🔗 URL
    url = f"{base_url}/api/reports/delivery/consolidated"
    params = {
        "key": token,
        "dateFrom": date_from,
        "dateTo": date_to
    }

    # 📡 Запрос
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            rows = []

            for row in root.findall(".//row"):
                row_data = {field.tag: field.text or "" for field in row}
                rows.append(row_data)

            df = pd.DataFrame(rows)

            # 🔄 Преобразуем типы данных
            df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")
            float_columns = [
                "orderCount", "avgReceipt", "orderCountCourier", "orderCountPickup",
                "revenue", "ratioCostWriteoff", "planExecutionPercent",
                "dishAmount", "dishAmountPerOrder"
            ]
            for col in float_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # 💽 Подключение к SQL Server
            cursor = conn.cursor()

            # 📌 Создание таблицы
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_delivery_consolidated', 'U') IS NULL
            CREATE TABLE dbo.stagging_table_iiko_delivery_consolidated (
                [date] DATE,
                orderCount FLOAT,
                avgReceipt FLOAT,
                orderCountCourier FLOAT,
                orderCountPickup FLOAT,
                revenue FLOAT,
                ratioCostWriteoff FLOAT,
                planExecutionPercent FLOAT,
                dishAmount FLOAT,
                dishAmountPerOrder FLOAT
            )
            """)
            conn.commit()

            # 🔁 Очистка и загрузка
            cursor.execute("DELETE FROM dbo.stagging_table_iiko_delivery_consolidated")
            conn.commit()

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dbo.stagging_table_iiko_delivery_consolidated (
                        [date], orderCount, avgReceipt, orderCountCourier, orderCountPickup,
                        revenue, ratioCostWriteoff, planExecutionPercent, dishAmount, dishAmountPerOrder
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row["date"], row["orderCount"], row["avgReceipt"], row["orderCountCourier"],
                row["orderCountPickup"], row["revenue"], row["ratioCostWriteoff"],
                row["planExecutionPercent"], row["dishAmount"], row["dishAmountPerOrder"]
                )

            conn.commit()
            cursor.close()
            print("✅ Данные загружены в SQL Server.")

        except ET.ParseError as e:
            print("❌ Ошибка XML:", e)
    else:
        print(f"❌ Запрос завершился с ошибкой: {response.status_code}")
