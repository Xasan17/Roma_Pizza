import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
from datetime import datetime, timedelta

urllib3.disable_warnings()

def load_employee_availability(token,conn):
    date_from =  (datetime.now() - timedelta(days=1461)).strftime("%Y-%m-%d")
    date_to = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


    def parse_date_safe(value):
        if not value:
            return None
        try:
            # Ko‘p hollarda ISO format bo‘ladi: '2023-07-15T09:00:00'
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            return None


    # API call
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/availability/list"
    params = {
        "key": token,
        "from": date_from,
        "to": date_to
    }

    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            availabilities = []
            for avail in root.findall('.//availability'):
                availabilities.append({
                    "employeeId": avail.findtext("employeeId"),
                    "dateFrom": avail.findtext("dateFrom"),
                    "dateTo": avail.findtext("dateTo")
                })

            df = pd.DataFrame(availabilities)

            # Connect to SQL Server
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_employee_availability', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_employee_availability (
                employeeId UNIQUEIDENTIFIER,
                dateFrom DATETIME,
                dateTo DATETIME
            )
            """)
            conn.commit()

            # Clear old data
            cursor.execute("DELETE FROM stagging_table_iiko_employee_availability")
            conn.commit()

            for row in df.to_dict('records'):
                if not row.get("employeeId"):
                    continue

                date_from = parse_date_safe(row.get("dateFrom"))
                date_to = parse_date_safe(row.get("dateTo"))

                cursor.execute("""
                    INSERT INTO stagging_table_iiko_employee_availability (
                        employeeId, dateFrom, dateTo
                    ) VALUES (?, ?, ?)
                """, (
                    row.get("employeeId"),
                    date_from,
                    date_to
                ))
            conn.commit()

            cursor.close()
            print("✅ Employee availability successfully loaded into SQL Server")

        except ET.ParseError as e:
            print("❌ Error parsing XML:", e)
    else:
        print(f"[!] Error {response.status_code}: {response.text}")
