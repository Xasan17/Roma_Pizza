import requests
import xml.etree.ElementTree as ET
import pandas as pd
import urllib3

# 🔕 Disable HTTPS warnings
urllib3.disable_warnings()

def load_attendance_types(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/employees/attendance/types"
    params = {
        "key": token,
        "includeDeleted": "false",
        "revisionFrom": "-1"
    }

    # 📡 API Request
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        try:
            root = ET.fromstring(response.text)
            attendance_types = []
            # Assuming response elements are <attendanceType>
            for atype in root.findall('.//attendanceType'):
                attendance_types.append({
                    "code": atype.findtext("code"),
                    "name": atype.findtext("name"),
                    "deleted": atype.findtext("deleted") == 'true'
                })

            # 📊 Convert to DataFrame
            df = pd.DataFrame(attendance_types)

            # 🛠 Connect to SQL Server
            cursor = conn.cursor()

            # ❗ Create table if not exists
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_attendance_types', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_attendance_types (
                code NVARCHAR(50) PRIMARY KEY,
                name NVARCHAR(255),
                deleted BIT
            )
            """)
            conn.commit()

            # 🧹 Clear old data
            cursor.execute("DELETE FROM stagging_table_iiko_attendance_types")
            conn.commit()

            # 📤 Insert data row-by-row
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stagging_table_iiko_attendance_types (code, name, deleted)
                    VALUES (?, ?, ?)
                """,
                row.get("code"),
                row.get("name"),
                int(row.get("deleted")))
            conn.commit()

            cursor.close()
            print("✅ Attendance types successfully loaded into SQL Server")

        except ET.ParseError as e:
            print("❌ Error parsing XML:", e)
    else:
        print(f"[!] Error {response.status_code}: {response.text}")
