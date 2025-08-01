import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
from datetime import datetime
import urllib3

# 🔕 Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_attendances(token,conn):
    date_from = datetime.now().strftime("%Y-%m-%d")
    date_to = date_from



    # 📡 Настройка запроса
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/attendance"
    params = {
        "key": token,
        "from": date_from,
        "to": date_to,
        "withPaymentDetails": "true",
        "revisionFrom": "-1"
    }

    # 🔄 Отправка запроса
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        root = ET.fromstring(response.text)
        rows = []

        for att in root.findall("attendance"):
            details = att.find("paymentDetails")

            rows.append({
                "id": att.findtext("id"),
                "employeeId": att.findtext("employeeId"),
                "roleId": att.findtext("roleId"),
                "dateFrom": att.findtext("dateFrom"),
                "dateTo": att.findtext("dateTo"),
                "attendanceType": att.findtext("attendanceType"),
                "departmentId": att.findtext("departmentId"),
                "departmentName": att.findtext("departmentName"),
                "regularPayedMinutes": details.findtext("regularPayedMinutes") if details is not None else None,
                "regularPaymentSum": details.findtext("regularPaymentSum") if details is not None else None,
                "overtimePayedMinutes": details.findtext("overtimePayedMinutes") if details is not None else None,
                "overtimePayedSum": details.findtext("overtimePayedSum") if details is not None else None,
                "otherPaymentsSum": details.findtext("otherPaymentsSum") if details is not None else None,
                "created": att.findtext("created"),
                "modified": att.findtext("modified")
            })

        df = pd.DataFrame(rows)

        # 🧱 Подключение к SQL Server
        cursor = conn.cursor()

        # ❗ Создание таблицы, если не существует
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_attendance', 'U') IS NULL
        CREATE TABLE dbo.stagging_table_attendance (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            employeeId UNIQUEIDENTIFIER,
            roleId UNIQUEIDENTIFIER,
            dateFrom DATETIME2,
            dateTo DATETIME2,
            attendanceType NVARCHAR(10),
            departmentId UNIQUEIDENTIFIER,
            departmentName NVARCHAR(255),
            regularPayedMinutes INT,
            regularPaymentSum FLOAT,
            overtimePayedMinutes INT,
            overtimePayedSum FLOAT,
            otherPaymentsSum FLOAT,
            created DATETIME2,
            modified DATETIME2
        )
        """)
        conn.commit()

        # 📤 Вставка новых данных (без удаления)
        for _, row in df.iterrows():
            # Проверка: есть ли уже запись с таким id
            cursor.execute("SELECT COUNT(*) FROM dbo.stagging_table_attendance WHERE id = ?", row.id)
            exists = cursor.fetchone()[0]
            if exists == 0:
                cursor.execute("""
                INSERT INTO dbo.stagging_table_attendance (
                    id, employeeId, roleId, dateFrom, dateTo, attendanceType,
                    departmentId, departmentName,
                    regularPayedMinutes, regularPaymentSum,
                    overtimePayedMinutes, overtimePayedSum, otherPaymentsSum,
                    created, modified
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row.id, row.employeeId, row.roleId, row.dateFrom, row.dateTo, row.attendanceType,
                    row.departmentId, row.departmentName,
                    row.regularPayedMinutes, row.regularPaymentSum,
                    row.overtimePayedMinutes, row.overtimePayedSum, row.otherPaymentsSum,
                    row.created, row.modified)

        conn.commit()
        print("✅ Посещаемость успешно добавлена в SQL Server (без удаления)")
    else:
        print("[!] Ошибка запроса:", response.status_code, response.text)
