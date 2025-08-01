import requests
import pandas as pd
import xml.etree.ElementTree as ET
import pyodbc
import urllib3
from datetime import datetime, timedelta

# 🚫 Отключить предупреждения SSL
urllib3.disable_warnings()

# 🔑 Настройки подключения
def load_delivery_consolidated(token,conn):
    base_url = "https://roma-pizza-co.iiko.it/resto"
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    today_date = today.strftime("%d.%m.%Y")
    tomorrow_date = tomorrow.strftime("%d.%m.%Y")

    url = f"{base_url}/api/reports/delivery/consolidated"
    params = {
        "key": token,
        "dateFrom": today_date,
        "dateTo": tomorrow_date
    }

    # 📡 Отправка запроса
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            rows = []

            for row in root.findall(".//row"):
                row_data = {field.tag: field.text or "" for field in row}
                rows.append(row_data)

            df = pd.DataFrame(rows)
            
            if df.empty:
                print("⚠️ Нет данных за сегодняшний день. Возможно, они еще не сформированы.")
                exit()
            # 📆 Преобразуем дату
            df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y")

            # 🔢 Числовые поля
            float_columns = [
                "orderCount", "avgReceipt", "orderCountCourier", "orderCountPickup",
                "revenue", "ratioCostWriteoff", "planExecutionPercent",
                "dishAmount", "dishAmountPerOrder"
            ]
            for col in float_columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            # 🔌 Подключение к SQL Server
            cursor = conn.cursor()

            # 🧱 Создание таблицы, если нет
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_delivery_consolidated', 'U') IS NULL
            CREATE TABLE dbo.stagging_table_iiko_delivery_consolidated (
                [date] DATE PRIMARY KEY,
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

            # 🔁 Обновление или вставка (MERGE по дате)
            for _, row in df.iterrows():
                cursor.execute("""
                    MERGE dbo.stagging_table_iiko_delivery_consolidated AS target
                    USING (SELECT ? AS [date]) AS source
                    ON target.[date] = source.[date]
                    WHEN MATCHED THEN
                        UPDATE SET 
                            orderCount = ?, avgReceipt = ?, orderCountCourier = ?, orderCountPickup = ?,
                            revenue = ?, ratioCostWriteoff = ?, planExecutionPercent = ?, 
                            dishAmount = ?, dishAmountPerOrder = ?
                    WHEN NOT MATCHED THEN
                        INSERT ([date], orderCount, avgReceipt, orderCountCourier, orderCountPickup,
                                revenue, ratioCostWriteoff, planExecutionPercent, dishAmount, dishAmountPerOrder)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, 
                    # source (дата)
                    row["date"],

                    # update часть
                    row["orderCount"], row["avgReceipt"], row["orderCountCourier"],
                    row["orderCountPickup"], row["revenue"], row["ratioCostWriteoff"],
                    row["planExecutionPercent"], row["dishAmount"], row["dishAmountPerOrder"],

                    # insert часть (в том же порядке)
                    row["date"], row["orderCount"], row["avgReceipt"], row["orderCountCourier"],
                    row["orderCountPickup"], row["revenue"], row["ratioCostWriteoff"],
                    row["planExecutionPercent"], row["dishAmount"], row["dishAmountPerOrder"]
                )

            conn.commit()
            cursor.close()

            print(f"✅ Загружено {len(df)} строк за {today}")

        except ET.ParseError as e:
            print("❌ Ошибка XML:", e)
    else:
        print(f"❌ Запрос завершился с ошибкой: {response.status_code}")
