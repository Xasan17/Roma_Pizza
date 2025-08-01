import requests
import xml.etree.ElementTree as ET
import pandas as pd
import urllib3
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
# 🔕 Disable HTTPS warnings
urllib3.disable_warnings()
def load_delivery_couriers_all(conn):
    token = get_iiko_token()
    # 🔐 Базовые настройки
    base_url = "https://roma-pizza-co.iiko.it/resto"
    start_date = datetime.strptime("01.01.2022", "%d.%m.%Y")
    end_date = datetime.now()

    # 🛠 Подключение к SQL Server
    cursor = conn.cursor()

    # ❗ Создать таблицу, если не существует
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

    # 🧹 Очистить таблицу (если нужно)
    cursor.execute("DELETE FROM stagging_table_iiko_couriers")
    conn.commit()

    # 📆 Перебираем даты по дням
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d.%m.%Y")
        next_day_str = (current_date + timedelta(days=1)).strftime("%d.%m.%Y")

        print(f"🔄 Загружаю данные за {date_str}...")

        url = f"{base_url}/api/reports/delivery/couriers"
        params = {
            "key": token,
            "dateFrom": date_str,
            "dateTo": next_day_str
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
                        "orderCount": float(metric.findtext("orderCount") or 0),
                        "date": current_date.date()
                    })

            if courier_data:
                df = pd.DataFrame(courier_data)

                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO stagging_table_iiko_couriers (
                            courier, metricType, totalTime, onTheWayTime,
                            doubledOrders, tripledOrders, orderCount, date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, row["courier"], row["metricType"], row["totalTime"],
                        row["onTheWayTime"], row["doubledOrders"],
                        row["tripledOrders"], row["orderCount"], row["date"])

                conn.commit()
        else:
            print(f"[!] Ошибка {response.status_code} за {date_str}: {response.text}")

        current_date += timedelta(days=1)

    cursor.close()
    print("✅ Все отчеты успешно загружены в SQL Server")
