import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime, timedelta
# 🔕 Disable HTTPS warnings
urllib3.disable_warnings()
def load_delivery_couriers(token,conn):
    base_url = "https://roma-pizza-co.iiko.it/resto"
    today = datetime.now()
    date_from = today.strftime("%d.%m.%Y")
    date_to = (today + timedelta(days=1)).strftime("%d.%m.%Y")
    
    # 📦 Request parameters
    url = f"{base_url}/api/reports/delivery/couriers"
    params = {
        "key": token,
        "dateFrom": date_from,
        "dateTo": date_to
    }

    response = requests.get(url, params=params, verify=False)

    if response.ok:
        root = ET.fromstring(response.content)

        courier_data = []
        for row in root.findall(".//row"):
            courier_name = row.findtext("courier")

            for metric in row.findall(".//metric"):
                metric_type = metric.findtext("metricType")

                courier_data.append({
                    "courier": courier_name,
                    "metricType": metric_type,
                    "totalTime": float(metric.findtext("totalTime") or 0),
                    "onTheWayTime": float(metric.findtext("onTheWayTime") or 0),
                    "doubledOrders": float(metric.findtext("doubleOrders") or 0),
                    "tripledOrders": float(metric.findtext("tripleOrders") or 0),
                    "orderCount": float(metric.findtext("orderCount") or 0)
                })

        df = pd.DataFrame(courier_data)

        # 🛠 Connect to SQL Server
        cursor = conn.cursor()

        # ❗ Create table if not exists
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_couriers', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_couriers (
            id INT IDENTITY(1,1) PRIMARY KEY,
            courier NVARCHAR(255),
            metricType NVARCHAR(50),
            totalTime FLOAT,
            onTheWayTime FLOAT,
            doubledOrders FLOAT,
            tripledOrders FLOAT,
            orderCount FLOAT,
            date DATE           
        )
        """)
        conn.commit()

        try:
            delete_sql = """
                DELETE FROM stagging_table_iiko_couriers
                WHERE CAST(date AS DATE) = ?
            """
            cursor.execute(delete_sql, (today.date(),))
            conn.commit()
        except Exception as e:
            print(f"[!] Ошибка при удалении сегодняшних данных: {e}")

        # 📤 Insert data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stagging_table_iiko_couriers (
                    courier, metricType, totalTime, onTheWayTime,
                    doubledOrders, tripledOrders, orderCount,date
                ) VALUES (?, ?, ?, ?, ?, ?, ?,?)
            """, row["courier"], row["metricType"], row["totalTime"],
                row["onTheWayTime"], row["doubledOrders"],
                row["tripledOrders"], row["orderCount"], today.date())

        conn.commit()
        cursor.close()

        print("✅ Отчет по курьерам успешно загружен в SQL Server")

    else:
        print(f"[!] Ошибка при запросе: {response.status_code} — {response.text}")
