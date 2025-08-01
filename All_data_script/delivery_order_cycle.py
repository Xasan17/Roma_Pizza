import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
# 🔕 Disable HTTPS warnings
urllib3.disable_warnings()
def load_delivery_order_cycle(conn):
    token = get_iiko_token()
    base_url = "https://roma-pizza-co.iiko.it/resto"
    date_from = "01.01.2022"
    date_to = datetime.now().strftime("%d.%m.%Y")

    # 📦 Request parameters
    url = f"{base_url}/api/reports/delivery/orderCycle"
    params = {
        "key": token,
        "dateFrom": date_from,
        "dateTo": date_to
    }

    response = requests.get(url, params=params, verify=False)

    if response.ok:
        root = ET.fromstring(response.content)

        cycle_data = []
        for row in root.findall(".//row"):
            cycle_data.append({
                "metricType": row.findtext("metricType") or "",
                "pizzaTime": float(row.findtext("pizzaTime") or 0),
                "cuttingTime": float(row.findtext("cuttingTime") or 0),
                "onShelfTime": float(row.findtext("onShelfTime") or 0),
                "inRestaurantTime": float(row.findtext("inRestaurantTime") or 0),
                "onTheWayTime": float(row.findtext("onTheWayTime") or 0),
                "totalTime": float(row.findtext("totalTime") or 0)
            })

        df = pd.DataFrame(cycle_data)

        # 🛠 Connect to SQL Server
        cursor = conn.cursor()

        # ❗ Create table if not exists
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_order_cycle', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_order_cycle (
            id INT IDENTITY(1,1) PRIMARY KEY,
            metricType NVARCHAR(50),
            pizzaTime FLOAT,
            cuttingTime FLOAT,
            onShelfTime FLOAT,
            inRestaurantTime FLOAT,
            onTheWayTime FLOAT,
            totalTime FLOAT
        )
        """)
        conn.commit()

        # 🧹 Clear old data
        cursor.execute("DELETE FROM stagging_table_iiko_order_cycle")
        conn.commit()

        # 📤 Insert data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stagging_table_iiko_order_cycle (
                    metricType, pizzaTime, cuttingTime, onShelfTime,
                    inRestaurantTime, onTheWayTime, totalTime
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            row["metricType"], row["pizzaTime"], row["cuttingTime"],
            row["onShelfTime"], row["inRestaurantTime"],
            row["onTheWayTime"], row["totalTime"])

        conn.commit()
        cursor.close()
        print("✅ Отчет по order cycle успешно загружен в SQL Server")

    else:
        print("[!] Ошибка при запросе:", response.status_code, response.text)
