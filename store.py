import requests
import pandas as pd
import urllib3
import xml.etree.ElementTree as ET
import pyodbc

urllib3.disable_warnings()

# 🔐 Токен и URL
token = "7230b187-1c18-8b4f-6520-568a88ed4c9d"
url = "https://roma-pizza-co.iiko.it/resto/api/corporation/stores"
params = {"key": token}

# 📡 Запрос к API
response = requests.get(url, params=params, verify=False)

if response.ok:
    root = ET.fromstring(response.content)

    stores = []
    for store in root.findall(".//corporateItemDto"):
        stores.append({
            "id": store.findtext("id"),
            "parentId": store.findtext("parentId"),
            "code": store.findtext("code"),
            "name": store.findtext("name").strip() if store.findtext("name") else "",
            "type": store.findtext("type")
        })

    df = pd.DataFrame(stores)
    df = df[df["type"] == "STORE"]  # ❗ Сохраняем только магазины

    # 🛠 Подключение к SQL Server
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=TA_GEO_07\\SQLEXPRESS;'
        'DATABASE=Roma_pizza;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # ❗ Таблица создаётся один раз
    cursor.execute("""
    IF OBJECT_ID('dbo.stagging_table_iiko_stores', 'U') IS NULL
    CREATE TABLE stagging_table_iiko_stores (
        id UNIQUEIDENTIFIER PRIMARY KEY,
        parentId UNIQUEIDENTIFIER,
        code NVARCHAR(255),
        name NVARCHAR(255),
        type NVARCHAR(50)
    )
    """)
    conn.commit()

    # 🧹 Очистим таблицу (если хочешь обновлять)
    cursor.execute("DELETE FROM stagging_table_iiko_stores")
    conn.commit()

    # 📤 Вставка данных
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stagging_table_iiko_stores (id, parentId, code, name, type)
            VALUES (?, ?, ?, ?, ?)
        """, row["id"], row["parentId"], row["code"], row["name"], row["type"])
    conn.commit()

    cursor.close()
    conn.close()

    print("✅ Список точек успешно загружен в SQL Server")

else:
    print("[!] Ошибка при запросе:", response.status_code, response.text)
