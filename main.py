import requests
import urllib3
import pandas as pd
import pyodbc

# Отключаем предупреждения
urllib3.disable_warnings()

# 🔐 Токен авторизации
token = "899827ad-1ad9-56f1-f780-7ae607d88f2c"

# 🌐 URL запроса
url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/quickLabels/list"
params = {
    "key": token,
    "includeDeleted": "false"
}

# 📥 Получаем данные
response = requests.get(url, params=params, verify=False)

if response.ok:
    data = response.json()

    # Распаковка вложенных "labels"
    rows = []
    for item in data:
        menu_id = item.get("id")
        depends_on_weekday = item.get("dependsOnWeekDay")
        department_id = item.get("departmentId")
        section_id = item.get("sectionId")
        page_names = item.get("pageNames", [])

        labels = item.get("labels", [])
        for label in labels:
            rows.append({
                "menu_id": menu_id,
                "dependsOnWeekDay": depends_on_weekday,
                "departmentId": department_id,
                "sectionId": section_id,
                "page": label.get("page"),
                "day": label.get("day"),
                "x": label.get("x"),
                "y": label.get("y"),
                "entityId": label.get("entityId"),
                "entityType": label.get("entityType"),
                "page_name_0": page_names[0] if len(page_names) > 0 else None,
                "page_name_1": page_names[1] if len(page_names) > 1 else None,
                "page_name_2": page_names[2] if len(page_names) > 2 else None
            })

    df = pd.DataFrame(rows)
    print(df.head())

    # 🔌 Подключение к SQL Server
    conn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'SERVER=TA_GEO_07\\SQLEXPRESS;'
        'DATABASE=Roma_pizza;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # 🧹 Очистка старой таблицы
    cursor.execute("""
        IF OBJECT_ID('dbo.stagging_quicklabels_iiko', 'U') IS NOT NULL
            DROP TABLE dbo.stagging_quicklabels_iiko
    """)
    conn.commit()

    # 🧱 Создание новой таблицы
    cursor.execute("""
        CREATE TABLE dbo.stagging_quicklabels_iiko (
            menu_id UNIQUEIDENTIFIER,
            dependsOnWeekDay BIT,
            departmentId UNIQUEIDENTIFIER,
            sectionId UNIQUEIDENTIFIER NULL,
            page INT,
            day INT,
            x INT,
            y INT,
            entityId UNIQUEIDENTIFIER,
            entityType NVARCHAR(50),
            page_name_0 NVARCHAR(100),
            page_name_1 NVARCHAR(100),
            page_name_2 NVARCHAR(100)
        )
    """)
    conn.commit()

    # 💾 Загрузка данных
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dbo.stagging_quicklabels_iiko (
                menu_id, dependsOnWeekDay, departmentId, sectionId,
                page, day, x, y,
                entityId, entityType,
                page_name_0, page_name_1, page_name_2
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row.menu_id, row.dependsOnWeekDay, row.departmentId, row.sectionId,
             row.page, row.day, row.x, row.y,
             row.entityId, row.entityType,
             row.page_name_0, row.page_name_1, row.page_name_2)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Данные QuickLabels успешно загружены в SQL Server!")

else:
    print(f"❌ Ошибка запроса: {response.status_code}\n{response.text}")
