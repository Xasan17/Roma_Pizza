import requests
import xml.etree.ElementTree as ET
import pyodbc
import urllib3

# 🔕 Отключаем HTTPS warning
urllib3.disable_warnings()
def load_events(token,conn):
    base_url = "https://roma-pizza-co.iiko.it"
    url = f"{base_url}/resto/api/events"
    params = {
        "key": token,
        "from_rev": "6405769"  # последняя ревизия
    }

    # 📡 Делаем запрос
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)

            # Сохраняем ревизию
            revision = root.findtext("revision")

            # ✅ Подключение к SQL Server
            cursor = conn.cursor()

            # ❗ Создаем таблицы если нет
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_events', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_events (
                event_id UNIQUEIDENTIFIER PRIMARY KEY,
                event_date DATETIMEOFFSET,
                event_type NVARCHAR(255),
                revision BIGINT
            )
            """)
            conn.commit()

            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_event_attributes', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_event_attributes (
                id INT IDENTITY PRIMARY KEY,
                event_id UNIQUEIDENTIFIER,
                attr_name NVARCHAR(255),
                attr_value NVARCHAR(MAX),
                attr_type NVARCHAR(255),
                FOREIGN KEY (event_id) REFERENCES stagging_table_iiko_events(event_id)
            )
            """)
            conn.commit()

            # 🧹 Очищаем старые данные (опционально)
            cursor.execute("DELETE FROM stagging_table_iiko_event_attributes")
            cursor.execute("DELETE FROM stagging_table_iiko_events")
            conn.commit()

            # ✅ Парсим все <event>
            for ev in root.findall('.//event'):
                event_id = ev.findtext("id")
                event_date = ev.findtext("date")
                event_type = ev.findtext("type")

                # 👉 1. Сохраняем сам event
                cursor.execute("""
                    INSERT INTO stagging_table_iiko_events (event_id, event_date, event_type, revision)
                    VALUES (?, ?, ?, ?)
                """, event_id, event_date, event_type, revision)

                # 👉 2. Сохраняем все attribute по отдельности
                for attr in ev.findall("attribute"):
                    name = attr.findtext("name")
                    value = attr.findtext("value")
                    attr_type = attr.findtext("type")

                    cursor.execute("""
                        INSERT INTO stagging_table_iiko_event_attributes (event_id, attr_name, attr_value, attr_type)
                        VALUES (?, ?, ?, ?)
                    """, event_id, name, value, attr_type)

            conn.commit()
            cursor.close()

            print("✅ Events + Attributes successfully saved to SQL Server")

        except ET.ParseError as e:
            print("❌ XML Parse Error:", e)
    else:
        print(f"[!] HTTP Error {response.status_code}: {response.text}")
