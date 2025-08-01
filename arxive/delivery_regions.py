import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime
# 🔕 Отключаем предупреждения SSL
urllib3.disable_warnings()

# 📡 Настройки подключения
base_url = "https://roma-pizza-co.iiko.it/resto"
token = "b30ed56c-316c-e69e-5e0b-d733535570ec"
date_from = "01.01.2022"
date_to = datetime.now().strftime("%d.%m.%Y")

# ✅ Корректный формат дат: YYYY-MM-DD
params = {
    "dateFrom": date_from,
    "dateTo": date_to,
    "key": token
}
url = f"{base_url}/api/reports/delivery/regions"

# 📤 Отправка запроса
response = requests.get(url, params=params, verify=False)

if response.ok:
    root = ET.fromstring(response.content)

    rows = root.findall(".//row")
    if not rows:
        print("⚠️ Данных за указанный период нет.")
    else:
        region_data = []
        for row in rows:
            region_data.append({
                "averageDeliveryTime": float(row.findtext("averageDeliveryTime") or 0),
                "deliveredOrdersPercent": float(row.findtext("deliveredOrdersPercent") or 0),
                "maxOrderCountDay": float(row.findtext("maxOrderCountDay") or 0),
                "orderCount": float(row.findtext("orderCount") or 0),
                "region": row.findtext("region") or ""
            })

        df = pd.DataFrame(region_data)

        # 🛠 Подключение к SQL Server
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=TA_GEO_07\\SQLEXPRESS;'
            'DATABASE=Roma_pizza;'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # ❗ Создание таблицы, если не существует
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_delivery_regions', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_delivery_regions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            averageDeliveryTime FLOAT,
            deliveredOrdersPercent FLOAT,
            maxOrderCountDay FLOAT,
            orderCount FLOAT,
            region NVARCHAR(100)
        )
        """)
        conn.commit()

        # 🧹 Очистка старых данных (если нужно)
        cursor.execute("DELETE FROM stagging_table_iiko_delivery_regions")
        conn.commit()

        # 📥 Загрузка новых данных
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stagging_table_iiko_delivery_regions (
                    averageDeliveryTime, deliveredOrdersPercent,
                    maxOrderCountDay, orderCount, region
                ) VALUES (?, ?, ?, ?, ?)
            """,
            row["averageDeliveryTime"], row["deliveredOrdersPercent"],
            row["maxOrderCountDay"], row["orderCount"], row["region"])

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Отчет по delivery regions успешно загружен в SQL Server")

else:
    print("[!] Ошибка при запросе:", response.status_code, response.text)
