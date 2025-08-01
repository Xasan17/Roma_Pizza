import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3

# 🔕 Отключаем предупреждения HTTPS
urllib3.disable_warnings()
def load_employee_roles(token,conn):
# 🔐 Токен и URL
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/roles"
    params = {
        "key": token,
        "includeDeleted": "false"
    }

    # 📡 API Request
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            # 🧾 Parse XML response
            root = ET.fromstring(response.text)

            roles = []
            for role in root.findall('.//role'):
                roles.append({
                    "id": role.findtext("id"),
                    "code": role.findtext("code"),
                    "name": role.findtext("name"),
                    "paymentPerHour": role.findtext("paymentPerHour"),
                    "steadySalary": role.findtext("steadySalary"),
                    "scheduleType": role.findtext("scheduleType"),
                    "deleted": role.findtext("deleted") == 'true'
                })

            # 📊 Convert to DataFrame
            df = pd.DataFrame(roles)

            # 🛠 Connect to SQL Server
            cursor = conn.cursor()

            # ❗ Create table if not exists
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_employee_roles', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_employee_roles (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                code NVARCHAR(50),
                name NVARCHAR(255),
                paymentPerHour FLOAT,
                steadySalary FLOAT,
                scheduleType NVARCHAR(50),
                deleted BIT
            )
            """)
            conn.commit()

            # 🧹 Clear old data (optional)
            cursor.execute("DELETE FROM stagging_table_iiko_employee_roles")
            conn.commit()

            # 📤 Insert role data row-by-row
            for _, row in df.iterrows():
                if not row.get("id"):
                    continue  # skip rows with null ID

                cursor.execute("""
                    INSERT INTO stagging_table_iiko_employee_roles (
                        id, code, name, paymentPerHour, steadySalary, scheduleType, deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                row.get("id"),
                row.get("code"),
                row.get("name"),
                float(row.get("paymentPerHour") or 0),
                float(row.get("steadySalary") or 0),
                row.get("scheduleType"),
                int(row.get("deleted")))

            conn.commit()
            cursor.close()
            print("✅ Роли сотрудников успешно загружены в SQL Server")

        except ET.ParseError as e:
            print("❌ Ошибка при разборе XML:", e)

    else:
        print(f"[!] Ошибка {response.status_code}: {response.text}")

