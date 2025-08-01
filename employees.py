import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3

# 🔕 Отключаем предупреждения HTTPS
urllib3.disable_warnings()

def load_employees(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/employees"
    params = {
        "key": token,
        "includeDeleted": "false"
    }

    # 📡 API Request
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        root = ET.fromstring(response.content)

        # 📥 Parse XML into list of dicts (только существующие поля)
        employees = []
        for emp in root.findall('.//employee'):
            employees.append({
                "id": emp.findtext("id"),
                "code": emp.findtext("code"),
                "name": emp.findtext("name"),
                "login": emp.findtext("login"),
                "mainRoleId": emp.findtext("mainRoleId"),
                "rolesIds": emp.findtext("rolesIds"),
                "mainRoleCode": emp.findtext("mainRoleCode"),
                "roleCodes": emp.findtext("roleCodes"),
                "cardNumber": emp.findtext("cardNumber"),
                "taxpayerIdNumber": emp.findtext("taxpayerIdNumber"),
                "snils": emp.findtext("snils"),
                "preferredDepartmentCode": emp.findtext("preferredDepartmentCode"),
                "departmentCodes": emp.findtext("departmentCodes"),
                "deleted": emp.findtext("deleted") == 'true',
                "supplier": emp.findtext("supplier") == 'true',
                "employee": emp.findtext("employee") == 'true',
                "client": emp.findtext("client") == 'true',
                "representsStore": emp.findtext("representsStore") == 'true',
                "publicExternalData": emp.findtext("publicExternalData") or ""
            })

        # 📊 Convert to DataFrame
        df = pd.DataFrame(employees)

        # 🛠 Connect to SQL Server
        cursor = conn.cursor()

        # ❗ Create table if not exists
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_employees', 'U') IS NULL
        CREATE TABLE stagging_table_iiko_employees (
            id NVARCHAR(100) PRIMARY KEY,
            code NVARCHAR(100),
            name NVARCHAR(255),
            login NVARCHAR(100),
            mainRoleId NVARCHAR(100),
            rolesIds NVARCHAR(MAX),
            mainRoleCode NVARCHAR(100),
            roleCodes NVARCHAR(MAX),
            cardNumber NVARCHAR(100),
            taxpayerIdNumber NVARCHAR(100),
            snils NVARCHAR(100),
            preferredDepartmentCode NVARCHAR(100),
            departmentCodes NVARCHAR(MAX),
            deleted BIT,
            supplier BIT,
            employee BIT,
            client BIT,
            representsStore BIT,
            publicExternalData NVARCHAR(MAX)
        )
        """)
        conn.commit()

        # 🧹 Clear existing data
        cursor.execute("DELETE FROM stagging_table_iiko_employees")
        conn.commit()

        # 📤 Insert rows
        for _, row in df.iterrows():
            if not row["id"]:
                continue

            cursor.execute("""
                INSERT INTO stagging_table_iiko_employees (
                    id, code, name, login, mainRoleId, rolesIds, mainRoleCode, roleCodes,
                    cardNumber, taxpayerIdNumber, snils, preferredDepartmentCode, departmentCodes,
                    deleted, supplier, employee, client, representsStore, publicExternalData
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            row.get("id"), row.get("code"), row.get("name"), row.get("login"),
            row.get("mainRoleId"), row.get("rolesIds"), row.get("mainRoleCode"), row.get("roleCodes"),
            row.get("cardNumber"), row.get("taxpayerIdNumber"), row.get("snils"),
            row.get("preferredDepartmentCode"), row.get("departmentCodes"),
            int(row.get("deleted", False)), int(row.get("supplier", False)),
            int(row.get("employee", False)), int(row.get("client", False)),
            int(row.get("representsStore", False)), row.get("publicExternalData"))

        conn.commit()
        cursor.close()

        print("✅ Сотрудники успешно загружены в SQL Server")

    else:
        print(f"[!] Ошибка при запросе: {response.status_code}\n{response.text}")
