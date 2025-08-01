import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
import uuid
from datetime import datetime
# 🔕 Отключаем предупреждения HTTPS
urllib3.disable_warnings()

def load_employees_salary(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/salary"
    date_from = "01.01.2022"
    date_to = datetime.now().strftime("%d.%m.%Y")
    params = {
        "key": token,
        "includeDeleted": "false",
        "dateFrom": date_from,
        "dateTo": date_to
    }

    # 📡 API Request
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            # 🧾 Parse XML
            root = ET.fromstring(response.text)

            salaries = []
            for sal in root.findall('.//salary'):
                salaries.append({
                    "id": str(uuid.uuid4()),  # Генерируем уникальный ID
                    "employeeId": sal.findtext("employeeId"),
                    "dateFrom": sal.findtext("dateFrom")[:10],  # Обрезаем до 'YYYY-MM-DD'
                    "payment": sal.findtext("payment"),
                })

            # 📊 DataFrame
            df = pd.DataFrame(salaries)

            # 🛠 SQL Server Connection
            cursor = conn.cursor()

            # 🧱 Create Table if Not Exists
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_employee_salaries', 'U') IS NULL
            CREATE TABLE dbo.stagging_table_iiko_employee_salaries (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                employeeId UNIQUEIDENTIFIER,
                dateFrom DATE,
                payment FLOAT
            )
            """)
            conn.commit()

            # 🧹 Clear existing data
            cursor.execute("DELETE FROM dbo.stagging_table_iiko_employee_salaries")
            conn.commit()

            # 📤 Insert Data
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO dbo.stagging_table_iiko_employee_salaries (
                        id, employeeId, dateFrom, payment
                    ) VALUES (?, ?, ?, ?)
                """,
                row["id"],
                row["employeeId"],
                row["dateFrom"],
                float(row["payment"]) if row["payment"] else 0.0)

            conn.commit()
            cursor.close()

            print("✅ Employee salaries successfully loaded into SQL Server.")

        except ET.ParseError as e:
            print("❌ XML Parse Error:", e)

    else:
        print(f"❌ API Request Failed: {response.status_code}\n{response.text}")
