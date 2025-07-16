import requests
import urllib3
import pandas as pd
import pyodbc

# 🔕 Отключаем предупреждения SSL
urllib3.disable_warnings()

# 🔐 Токен авторизации
token = "9a09aae9-6d70-8453-3c25-96a6aafab6cb"

# 🌐 URL запроса
url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/accounts/list"
params = {
    "key": token,
    "includeDeleted": "false"
}

# 📥 Запрос к API
response = requests.get(url, params=params, verify=False)

if response.ok:
    data = response.json()

    # 📊 Преобразование в DataFrame
    df = pd.DataFrame(data)

    # 🔌 Подключение к SQL Server
    conn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'SERVER=TA_GEO_07\\SQLEXPRESS;'
        'DATABASE=Roma_pizza;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # 🧹 Удаление старой таблицы, если она есть
    cursor.execute("""
        IF OBJECT_ID('dbo.stagging_accounts_iiko', 'U') IS NOT NULL
            DROP TABLE dbo.stagging_accounts_iiko
    """)
    conn.commit()

    # 🧱 Создание новой таблицы
    cursor.execute("""
        CREATE TABLE dbo.stagging_accounts_iiko (
            id UNIQUEIDENTIFIER,
            accountParentId UNIQUEIDENTIFIER NULL,
            parentCorporateId UNIQUEIDENTIFIER NULL,
            code NVARCHAR(50),
            deleted BIT,
            name NVARCHAR(255),
            type NVARCHAR(50),
            system BIT,
            customTransactionsAllowed BIT
        )
    """)
    conn.commit()

    # 💾 Загрузка данных
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dbo.stagging_accounts_iiko (
                id, accountParentId, parentCorporateId,
                code, deleted, name, type, system, customTransactionsAllowed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            row.get("id"),
            row.get("accountParentId"),
            row.get("parentCorporateId"),
            row.get("code"),
            row.get("deleted"),
            row.get("name"),
            row.get("type"),
            row.get("system"),
            row.get("customTransactionsAllowed")
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Данные Accounts успешно загружены в SQL Server!")

else:
    print(f"❌ Ошибка запроса: {response.status_code}\n{response.text}")
