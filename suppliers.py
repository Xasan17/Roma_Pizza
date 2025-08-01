import requests
import xml.etree.ElementTree as ET
import pandas as pd
import urllib3


urllib3.disable_warnings()
def load_supliers(token,conn):
    # 🔑 API key как переменная
    url = "https://roma-pizza-co.iiko.it/resto/api/suppliers"
    params = {
        "key": token,
        "includeDeleted": "true",
        "revisionFrom": "-1"
    }

    # 🔄 Запрос
    response = requests.get(url, params=params, verify=False)
    if not response.ok:
        print(f"[!] Ошибка {response.status_code}: {response.text}")
        exit()

    try:
        root = ET.fromstring(response.text)
        suppliers_list = []

        def safe_find(elem, tag):
            node = elem.find(tag)
            text = node.text.strip() if node is not None and node.text else None
            return text

        # ✅ Корректный парсинг только employee
        for emp in root.findall('employee'):
            suppliers_list.append({
                "id": safe_find(emp, "id"),
                "code": safe_find(emp, "code"),
                "name": safe_find(emp, "name"),
                "login": safe_find(emp, "login"),
                "taxpayerIdNumber": safe_find(emp, "taxpayerIdNumber"),
                "deleted": safe_find(emp, "deleted") == "true",
                "supplier": safe_find(emp, "supplier") == "true",
                "employee": safe_find(emp, "employee") == "true",
                "client": safe_find(emp, "client") == "true",
                "representsStore": safe_find(emp, "representsStore") == "true",
                "representedStoreId": safe_find(emp, "representedStoreId"),
                "publicExternalData": safe_find(emp, "publicExternalData")
            })

        df = pd.DataFrame(suppliers_list)
        # 🔌 Подключение к SQL Server
        cursor = conn.cursor()

        # 🧱 Создание таблицы если нет
        cursor.execute("""
        IF OBJECT_ID('dbo.staging_iiko_suppliers', 'U') IS NULL
        CREATE TABLE dbo.staging_iiko_suppliers (
            id UNIQUEIDENTIFIER,
            code NVARCHAR(50),
            name NVARCHAR(255),
            login NVARCHAR(100),
            taxpayerIdNumber NVARCHAR(50),
            deleted BIT,
            supplier BIT,
            employee BIT,
            client BIT,
            representsStore BIT,
            representedStoreId UNIQUEIDENTIFIER,
            publicExternalData NVARCHAR(MAX)
        )
        """)
        conn.commit()

        # 🧹 Очистка таблицы
        cursor.execute("DELETE FROM dbo.staging_iiko_suppliers")
        conn.commit()

        # ⬆ Загрузка данных
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dbo.staging_iiko_suppliers (
                    id, code, name, login, taxpayerIdNumber,
                    deleted, supplier, employee, client, representsStore,
                    representedStoreId, publicExternalData
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row["id"],
            row["code"],
            row["name"],
            row["login"],
            row["taxpayerIdNumber"],
            int(row["deleted"]) if row["deleted"] is not None else 0,
            int(row["supplier"]) if row["supplier"] is not None else 0,
            int(row["employee"]) if row["employee"] is not None else 0,
            int(row["client"]) if row["client"] is not None else 0,
            int(row["representsStore"]) if row["representsStore"] is not None else 0,
            row["representedStoreId"],
            row["publicExternalData"])
        

        conn.commit()
        cursor.close()
        print("✅ Данные успешно загружены в dbo.staging_iiko_suppliers")

    except ET.ParseError as e:
        print("❌ Ошибка парсинга XML:", e)
