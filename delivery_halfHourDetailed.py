import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import urllib3
import pyodbc

# 🔕 Отключить предупреждения SSL
urllib3.disable_warnings()
def load_delivery_halfHour(token,conn):
# 📦 Параметры
    base_url = "https://roma-pizza-co.iiko.it/resto"
    url = f"{base_url}/api/reports/delivery/halfHourDetailed"

    # 🗓️ Сегодняшняя дата
    today = datetime.now().date()
    date_from = today.strftime("%d.%m.%Y")
    date_to = (today + timedelta(days=1)).strftime("%d.%m.%Y")

    # 📊 Хранилище данных
    all_data = []

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
        except Exception as e:
            print(f"[!] Ошибка XML: {e}")
    else:
        print(f"[!] Ошибка HTTP: {response.status_code} {response.text}")

    # 📄 DataFrame
    df = pd.DataFrame(all_data)
    df["halfHourDate"] = pd.to_datetime(df["halfHourDate"], format="%d.%m.%Y %H:%M", errors="coerce")
    df = df.dropna(subset=["halfHourDate"])

    # 🧱 Подключение к SQL Server
    cursor = conn.cursor()

    # 🗑️ Удаляем старые данные за сегодня
    cursor.execute("""
        DELETE FROM stagging_table_iiko_half_hour
        WHERE CAST(halfHourDate AS DATE) = ?
    """, today)
    conn.commit()

    # 💾 Вставляем новые данные
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

    print(f"✅ Загружено записей за {today}: {len(df)}")
