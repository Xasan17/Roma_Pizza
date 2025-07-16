import requests
import pandas as pd
import pyodbc
import urllib3

# Отключаем предупреждения SSL
urllib3.disable_warnings()

# 🔐 Токен авторизации
token = "899827ad-1ad9-56f1-f780-7ae607d88f2c"

# 📦 URL запроса
url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/priceCategories"

# 📥 Отправка GET-запроса
response = requests.get(url, params={"key": token}, verify=False)

# ✅ Проверка ответа
if response.ok:
    try:
        json_data = response.json()
        categories = json_data.get("response", [])

        # 📊 Преобразование в DataFrame
        df = pd.json_normalize(categories, sep='_')

        print("🔍 Загружены данные:", df.shape)

        # 🔧 Подключение к SQL Server
        conn = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};'
            'SERVER=TA_GEO_07\\SQLEXPRESS;'
            'DATABASE=Roma_pizza;'
            'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()

        # 🧹 Удаление таблицы, если существует
        cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_price_categories', 'U') IS NOT NULL
                DROP TABLE dbo.stagging_table_iiko_price_categories;
        """)
        conn.commit()

        # 🏗️ Создание таблицы
        cursor.execute("""
            CREATE TABLE dbo.stagging_table_iiko_price_categories (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                name NVARCHAR(255),
                code NVARCHAR(50),
                deleted BIT,
                assignableManually BIT,
                strategyType NVARCHAR(50),
                strategyValue DECIMAL(10, 2)
            )
        """)
        conn.commit()

        # 💾 Вставка данных
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dbo.stagging_table_iiko_price_categories (
                    id, name, code, deleted, assignableManually, strategyType, strategyValue
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            row.get("id"),
            row.get("name"),
            row.get("code"),
            row.get("deleted"),
            row.get("assignableManually"),
            row.get("pricingStrategy_type"),
            row.get("pricingStrategy_percent", row.get("pricingStrategy_delta", 0))
        )

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Ценовые категории успешно загружены в SQL Server.")

    except Exception as e:
        print("❌ Ошибка при обработке данных:", e)
else:
    print(f"❌ Ошибка {response.status_code}: {response.text}")
