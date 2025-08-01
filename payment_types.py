import requests
import urllib3
import pandas as pd

urllib3.disable_warnings()
def load_payment_types(token,conn):
    # 📦 URL для справочника типов оплат
    url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/list"
    params = {
        "key": token,
        "rootType": "PaymentType",
        "includeDeleted": "false"
    }

    # 📥 Отправка запроса
    response = requests.get(url, params=params, verify=False)

    # ✅ Обработка результата
    if response.ok:
        data = response.json()
        df = pd.json_normalize(data, sep='_')

        # Подключение к SQL Server
        cursor = conn.cursor()

        # Удаляем старую таблицу
        cursor.execute("""
            IF OBJECT_ID('dbo.stagging_table_iiko_payments_type', 'U') IS NOT NULL
                DROP TABLE dbo.stagging_table_iiko_payments_type
        """)
        conn.commit()

        # Создаём новую таблицу
        cursor.execute("""
            CREATE TABLE dbo.stagging_table_iiko_payments_type (
                id UNIQUEIDENTIFIER,
                rootType NVARCHAR(50),
                deleted BIT,
                code NVARCHAR(100),
                name NVARCHAR(255)
            )
        """)
        conn.commit()

        # Вставляем данные
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dbo.stagging_table_iiko_payments_type (
                    id, rootType, deleted, code, name
                ) VALUES (?, ?, ?, ?, ?)
            """,
                row.get("id"),
                row.get("rootType"),
                row.get("deleted"),
                row.get("code"),
                row.get("name")
            )

        conn.commit()
        cursor.close()
        print("✅ Типы оплат успешно загружены в SQL Server")

    else:
        print(f"❌ Ошибка {response.status_code}: {response.text}")
