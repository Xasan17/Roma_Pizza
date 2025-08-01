import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime, timedelta

# 🔕 Отключаем предупреждения
urllib3.disable_warnings()
def load_employee_shifts(token,conn):
    date_from = "2022-01-01"
    date_to = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    # 🌐 URL и параметры запроса
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/schedule/"
    params = {
        "key": token,
        "from": date_from,
        "to": date_to,
        "withPaymentDetails": "false",
        "revisionFrom": "-1"
    }

    # 📡 Запрос к API
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            shifts = []

            for s in root.findall('.//schedule'):
                shifts.append({
                    "id": s.findtext("id"),
                    "employeeId": s.findtext("employeeId"),
                    "roleId": s.findtext("roleId"),
                    "dateFrom": s.findtext("dateFrom"),
                    "dateTo": s.findtext("dateTo"),
                    "scheduleTypeCode": s.findtext("scheduleTypeCode"),
                    "scheduleTypeId": s.findtext("scheduleTypeId"),  # 👈 добавили
                    "nonPaidMinutes": int(s.findtext("nonPaidMinutes") or 0),
                    "departmentId": s.findtext("departmentId"),
                    "departmentName": s.findtext("departmentName")
                })

            df = pd.DataFrame(shifts)

            # 🧱 Подключение к SQL Server
            cursor = conn.cursor()

            # ❗ Создание таблицы (если не существует)
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_employee_shifts', 'U') IS NULL
            CREATE TABLE dbo.stagging_table_iiko_employee_shifts (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                employeeId UNIQUEIDENTIFIER,
                roleId UNIQUEIDENTIFIER,
                dateFrom DATETIME2,
                dateTo DATETIME2,
                scheduleTypeCode NVARCHAR(50),
                scheduleTypeId NVARCHAR(50), 
                nonPaidMinutes INT,
                departmentId UNIQUEIDENTIFIER,
                departmentName NVARCHAR(255)
            )
            """)
            conn.commit()

            # 🧹 Очистка старых данных
            cursor.execute("DELETE FROM dbo.stagging_table_iiko_employee_shifts")
            conn.commit()

            # 📤 Вставка данных
            for _, row in df.iterrows():
                if not row.get("id"):
                    continue

                cursor.execute("""
                    INSERT INTO dbo.stagging_table_iiko_employee_shifts (
                        id, employeeId, roleId, dateFrom, dateTo,
                        scheduleTypeCode, scheduleTypeId, nonPaidMinutes,
                        departmentId, departmentName
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row.get("id"),
                row.get("employeeId"),
                row.get("roleId"),
                row.get("dateFrom"),
                row.get("dateTo"),
                row.get("scheduleTypeCode"),
                row.get("scheduleTypeId"),
                row.get("nonPaidMinutes"),
                row.get("departmentId"),
                row.get("departmentName"))

            conn.commit()
            cursor.close()

            print("✅ Смены сотрудников успешно загружены в SQL Server")

        except ET.ParseError as e:
            print("❌ Ошибка при разборе XML:", e)
    else:
        print(f"[!] Ошибка {response.status_code}: {response.text}")
