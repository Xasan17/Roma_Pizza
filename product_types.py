import requests
import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_product_types(token,conn):
    url = "https://roma-pizza-co.iiko.it/resto/api/products"
    params = {
        "key": token,
        "includeDeleted": "false",
        "revisionFrom": "-1"
    }

    # 📡 Отправка запроса
    response = requests.get(url, params=params, verify=False)

    if response.ok:
        root = ET.fromstring(response.text)
        rows = []

        for product in root.findall('productDto'):
            rows.append({
                "id": product.findtext("id"),
                "num": product.findtext("num"),
                "code": product.findtext("code"),
                "name": product.findtext("name"),
                "productType": product.findtext("productType"),
                "cookingPlaceType": product.findtext("cookingPlaceType"),
                "mainUnit": product.findtext("mainUnit"),
                "productCategory": product.findtext("productCategory")
            })

        df = pd.DataFrame(rows)

        # 🧱 Подключение к SQL Server
        cursor = conn.cursor()

        # ❗ Создание таблицы
        cursor.execute("""
        IF OBJECT_ID('dbo.stagging_table_iiko_product_type', 'U') IS NULL
        CREATE TABLE dbo.stagging_table_iiko_product_type (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            num NVARCHAR(50),
            code NVARCHAR(50),
            name NVARCHAR(255),
            productType NVARCHAR(50),
            cookingPlaceType NVARCHAR(100),
            mainUnit NVARCHAR(50),
            productCategory NVARCHAR(255)
        )
        """)
        conn.commit()

        # 🧹 Очистка
        cursor.execute("DELETE FROM dbo.stagging_table_iiko_product_type")
        conn.commit()

        # 📤 Загрузка
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO dbo.stagging_table_iiko_product_type (
                id, num, code, name, productType, cookingPlaceType, mainUnit, productCategory
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row.id, row.num, row.code, row.name, row.productType, row.cookingPlaceType, row.mainUnit,
                        row.productCategory)
        conn.commit()
        print("✅ Продукты успешно загружены в SQL Server")

    else:
        print("[!] Ошибка:", response.status_code, response.text)
