import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3

urllib3.disable_warnings()
def load_schedule_types(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/schedule/types"
    params = {
        "key": token
    }

    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            schedule_types = []

            for type_ in root.findall('.//scheduleType'):
                schedule_types.append({
                    "id": type_.findtext("id"),
                    "code": type_.findtext("code"),
                    "name": type_.findtext("name"),
                    "startTime": type_.findtext("startTime"),
                    "lengthMinutes": type_.findtext("lengthMinutes"),
                    "comment": type_.findtext("comment"),
                    "overtime": type_.findtext("overtime") == 'true',
                    "deleted": type_.findtext("deleted") == 'true'
                })

            df = pd.DataFrame(schedule_types)

            # 🔌 SQL-подключение
            cursor = conn.cursor()

            # 🧱 Создание таблицы, если не существует
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_schedule_types', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_schedule_types (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                code NVARCHAR(50),
                name NVARCHAR(255),
                startTime TIME,
                lengthMinutes INT,
                comment NVARCHAR(MAX),
                overtime BIT,
                deleted BIT
            )
            """)
            conn.commit()

            # 🧹 Очистка таблицы перед вставкой
            cursor.execute("DELETE FROM stagging_table_iiko_schedule_types")
            conn.commit()

            # 📝 Вставка данных
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stagging_table_iiko_schedule_types (
                        id, code, name, startTime, lengthMinutes, comment, overtime, deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row.get("id"),
                row.get("code"),
                row.get("name"),
                row.get("startTime"),
                int(row.get("lengthMinutes")) if pd.notnull(row.get("lengthMinutes")) else None,
                row.get("comment"),
                int(row.get("overtime")),
                int(row.get("deleted")))

            conn.commit()
            cursor.close()

            print("✅ Все типы смен успешно загружены в SQL Server")

        except ET.ParseError as e:
            print("❌ Ошибка при разборе XML:", e)

    else:
        print(f"[!] Ошибка {response.status_code}: {response.text}")
