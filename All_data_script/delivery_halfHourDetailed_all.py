import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import urllib3
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iiko_auth import get_iiko_token
# 🔕 Отключить предупреждения SSL
urllib3.disable_warnings()
def load_delivery_halfHourDetailed_all(conn):
    token = get_iiko_token()
    # 📦 Параметры
    base_url = "https://roma-pizza-co.iiko.it/resto"
    url = f"{base_url}/api/reports/delivery/halfHourDetailed"

    # 🗓️ Диапазон дат
    start_date = datetime.strptime("01.01.2022", "%d.%m.%Y").date()
    end_date = datetime.now().date()

    # 📊 Хранилище данных
    all_data = []

    # ⏱️ По дням
    current = start_date
    while current < end_date:
        date_from = current.strftime("%d.%m.%Y")
        date_to = (current + timedelta(days=1)).strftime("%d.%m.%Y")

        print(f"📆 Загружаем за {date_from}...")
        params = {
            "key": token,
            "dateFrom": date_from,
            "dateTo": date_to
        }

        response = requests.get(url, params=params, verify=False)

        if response.ok:
            try:
                root = ET.fromstring(response.content)
                for row in root.findall(".//row"):
                    half_hour = row.findtext("halfHourDate")
                    for metric in row.findall(".//metric"):
                        all_data.append({
                            "halfHourDate": half_hour,
                            "avgDishAmountPerReceipt": float(metric.findtext("avgDishAmountPerReceipt") or 0),
                            "avgReceipt": float(metric.findtext("avgReceipt") or 0),
                            "deliveryType": metric.findtext("deliveryType"),
                            "dishAmount": float(metric.findtext("dishAmount") or 0),
                            "orderCount": float(metric.findtext("orderCount") or 0)
                        })
                print(f"✅ Успешно")
            except Exception as e:
                print(f"[!] Ошибка XML на {date_from}: {e}")
        else:
            print(f"[!] Ошибка HTTP на {date_from}: {response.status_code} {response.text}")

        current += timedelta(days=1)

    # 📄 DataFrame
    df = pd.DataFrame(all_data)
    df["halfHourDate"] = pd.to_datetime(df["halfHourDate"], format="%d.%m.%Y %H:%M", errors="coerce")
    df = df.dropna(subset=["halfHourDate"])

    # 🧱 SQL Server
    cursor = conn.cursor()

    # 🏗️ Таблица
    cursor.execute("""
    IF OBJECT_ID('dbo.stagging_table_iiko_half_hour', 'U') IS NULL
    CREATE TABLE stagging_table_iiko_half_hour (
        id INT IDENTITY(1,1) PRIMARY KEY,
        halfHourDate DATETIME,
        avgDishAmountPerReceipt FLOAT,
        avgReceipt FLOAT,
        deliveryType NVARCHAR(50),
        dishAmount FLOAT,
        orderCount FLOAT
    )
    """)
    conn.commit()

    # 🔄 Очистка таблицы
    cursor.execute("DELETE FROM stagging_table_iiko_half_hour")
    conn.commit()

    # 💾 Загрузка в БД
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stagging_table_iiko_half_hour (
                halfHourDate, avgDishAmountPerReceipt, avgReceipt,
                deliveryType, dishAmount, orderCount
            ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        row["halfHourDate"], row["avgDishAmountPerReceipt"], row["avgReceipt"],
        row["deliveryType"], row["dishAmount"], row["orderCount"])

    conn.commit()
    cursor.close()

    print(f"🏁 Всего загружено: {len(df)} записей.")
