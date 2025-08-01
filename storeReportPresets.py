import requests
import pandas as pd
import xml.etree.ElementTree as ET
import urllib3

# 🚫 Disable SSL warnings
urllib3.disable_warnings()
def load_store_report_presets(token,conn):
    base_url = "https://roma-pizza-co.iiko.it/resto"
    # 📤 Отправка запроса
    url = f"{base_url}/api/reports/storeReportPresets"
    params = {"key": token}
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            rows = []

            for preset in root.findall('.//storeReportPreset'):
                # Читаем вложенные transactionTypes
                transaction_types = [i.text for i in preset.findall(".//transactionTypes/i")]
                data_direction = preset.findtext(".//dataDirection")
                comment = preset.findtext("comment")

                rows.append({
                    "id": preset.findtext("id"),
                    "name": preset.findtext("name"),
                    "comment": comment,
                    "dataDirection": data_direction,
                    "transactionTypes": ", ".join(transaction_types)
                })

            # 📊 В DataFrame
            df = pd.DataFrame(rows)

            # ✅ Подключение к SQL
            cursor = conn.cursor()

            # 🏗 Создание таблицы
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_store_report_presets', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_store_report_presets (
                id NVARCHAR(100) PRIMARY KEY,
                name NVARCHAR(255),
                comment NVARCHAR(MAX),
                dataDirection NVARCHAR(50),
                transactionTypes NVARCHAR(MAX)
            )
            """)
            conn.commit()

            # ❌ Очистка
            cursor.execute("DELETE FROM stagging_table_iiko_store_report_presets")
            conn.commit()

            # 📥 Загрузка данных
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stagging_table_iiko_store_report_presets
                    (id, name, comment, dataDirection, transactionTypes)
                    VALUES (?, ?, ?, ?, ?)
                """, row["id"], row["name"], row["comment"], row["dataDirection"], row["transactionTypes"])
            conn.commit()

            cursor.close()
            print("✅ Store Report Presets saved successfully!")

        except ET.ParseError as e:
            print("❌ XML Parsing Error:", e)
    else:
        print(f"❌ Request failed: {response.status_code} — {response.text}")
