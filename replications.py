import requests
import xml.etree.ElementTree as ET
import urllib3

# 🔕 Отключаем HTTPS warning
urllib3.disable_warnings()
def load_replications(token,conn):
    base_url = "https://roma-pizza-co.iiko.it"
    url = f"{base_url}/resto/api/replication/statuses"
    params = {
        "key": token
    }

    # 📡 Делаем запрос
    response = requests.get(url, params=params, verify=False)


    if response.ok:
        try:
            root = ET.fromstring(response.text)

            # ✅ Подключение к SQL Server
            cursor = conn.cursor()

            # ❗ Создаем таблицу, если ее нет
            cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_replications', 'U') IS NULL
            CREATE TABLE stagging_table_iiko_replications (
                department_id UNIQUEIDENTIFIER,
                department_name NVARCHAR(255),
                last_receive_date DATETIMEOFFSET,
                last_send_date DATETIMEOFFSET,
                status NVARCHAR(50)
            )
            """)
            conn.commit()

            # 🧹 Очищаем старые данные (опционально)
            cursor.execute("DELETE FROM stagging_table_iiko_replications")
            conn.commit()

            # ✅ Парсим все <replicationStatusDto>
            for rep in root.findall('.//replicationStatusDto'):
                department_id = rep.findtext("departmentId")
                department_name = rep.findtext("departmentName")
                last_receive_date = rep.findtext("lastReceiveDate")
                last_send_date = rep.findtext("lastSendDate")
                status = rep.findtext("status")

                # 👉 Сохраняем в SQL
                cursor.execute("""
                    INSERT INTO stagging_table_iiko_replications 
                    (department_id, department_name, last_receive_date, last_send_date, status)
                    VALUES (?, ?, ?, ?, ?)
                """, department_id, department_name, last_receive_date, last_send_date, status)

            conn.commit()
            cursor.close()

            print("✅ Репликации успешно сохранены в SQL Server!")

        except ET.ParseError as e:
            print("❌ XML Parse Error:", e)
    else:
        print(f"[!] HTTP Error {response.status_code}: {response.text}")
