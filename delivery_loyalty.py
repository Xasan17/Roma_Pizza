import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings()
def load_delivery_loyalty(token,conn):
    base_url = "https://roma-pizza-co.iiko.it/resto"
    date_from = datetime.now().strftime("%d.%m.%Y")
    date_to = (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y")
    metric_types = ["MAXIMUM", "AVERAGE"]

    loyalty_data = []

    for metric in metric_types:
        url = f"{base_url}/api/reports/delivery/loyalty"
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "metricType": metric,
            "key": token
        }

        response = requests.get(url, params=params, verify=False)

        if response.ok:
            try:
                root = ET.fromstring(response.content)
                for row in root.findall(".//row"):
                    date = row.findtext("date")
                    metric_type = row.findtext("metricType")
                    new_guest_count = float(row.findtext("newGuestCount") or 0)
                    order_count_per_guest = float(row.findtext("orderCountPerGuest") or 0)
                    total_order_count = float(row.findtext("totalOrderCount") or 0)

                    for region in row.findall(".//region"):
                        loyalty_data.append({
                            "date": date,
                            "metricType": metric_type,
                            "newGuestCount": new_guest_count,
                            "orderCountPerGuest": order_count_per_guest,
                            "totalOrderCount": total_order_count,
                            "region": region.findtext("region") or "Неизвестно",
                            "regionOrderCount": float(region.findtext("orderCount") or 0)
                        })
            except Exception as e:
                print(f"[!] Ошибка разбора XML для {metric}: {e}")
        else:
            print(f"[!] HTTP ошибка для {metric}: {response.status_code} {response.text}")

    # 📄 DataFrame
    df = pd.DataFrame(loyalty_data)
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")

    # 🛠 Подключение к SQL Server
    cursor = conn.cursor()

    # 🏗️ Создание таблицы, если не существует
    cursor.execute("""
    IF OBJECT_ID('dbo.stagging_table_iiko_delivery_loyalty', 'U') IS NULL
    BEGIN
        CREATE TABLE stagging_table_iiko_delivery_loyalty (
            id INT IDENTITY(1,1) PRIMARY KEY,
            [date] DATE,
            metricType NVARCHAR(20),
            newGuestCount FLOAT,
            orderCountPerGuest FLOAT,
            totalOrderCount FLOAT,
            region NVARCHAR(50),
            regionOrderCount FLOAT
        )
    END
    """)
    conn.commit()

    # 🧹 Удаление только по нужным датам
    unique_dates = df["date"].dropna().dt.date.unique()
    for dt in unique_dates:
        cursor.execute("""
            DELETE FROM stagging_table_iiko_delivery_loyalty
            WHERE [date] = ?
        """, dt)
    conn.commit()

    # 💾 Загрузка в таблицу
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stagging_table_iiko_delivery_loyalty (
                [date], metricType, newGuestCount, orderCountPerGuest,
                totalOrderCount, region, regionOrderCount
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        row["date"], row["metricType"], row["newGuestCount"],
        row["orderCountPerGuest"], row["totalOrderCount"],
        row["region"], row["regionOrderCount"])

    conn.commit()
    cursor.close()

    print(f"✅ Загружено {len(df)} строк loyalty данных по метрикам: {', '.join(metric_types)}")
