import requests
import pandas as pd
import pyodbc
from datetime import datetime

# === НАСТРОЙКИ ===
IIKO_HOST = "https://roma-pizza-co.iiko.it"
USERNAME = "Admin3"               # замените на свой
PASSWORD = "456321"           # замените на свой
SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=TA_GEO_07\\SQLEXPRESS;"
    "Database=roma_pizza_db;"
    "Trusted_Connection=yes;"
)

# === АВТОРИЗАЦИЯ ===
auth_response = requests.post(
    f"{IIKO_HOST}/resto/api/auth",
    auth=(USERNAME, PASSWORD),
    verify=False
)
cookie = auth_response.cookies

# === ЗАПРОС ДАННЫХ О СМЕНАХ ===
params = {
    "openDateFrom": "2024-01-01",
    "openDateTo": datetime.now().strftime("%Y-%m-%d"),
    "status": "ANY"
}
response = requests.get(
    f"{IIKO_HOST}/resto/api/v2/cashshifts/list",
    cookies=cookie,
    params=params,
    verify=False
)

try:
    data = response.json()
except Exception as e:
    print("Ошибка при разборе JSON:", e)
    print("Ответ сервера:", response.text)
    exit(1)

# === ПРЕОБРАЗОВАНИЕ В DataFrame ===
df = pd.DataFrame(data)

# === ПРИВЕДЕНИЕ ТИПОВ ===
def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

# === СОЕДИНЕНИЕ С SQL Server ===
conn = pyodbc.connect(SQL_CONN_STR)
cursor = conn.cursor()

# === СОЗДАНИЕ ТАБЛИЦЫ (если нужно) ===
cursor.execute("""
IF OBJECT_ID('dbo.iiko_cash_shifts_fact', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.iiko_cash_shifts_fact (
        id UNIQUEIDENTIFIER PRIMARY KEY,
        sessionNumber INT,
        fiscalNumber INT,
        cashRegNumber INT,
        cashRegSerial NVARCHAR(50),
        openDate DATETIME,
        closeDate DATETIME,
        acceptDate DATETIME,
        managerId UNIQUEIDENTIFIER,
        responsibleUserId UNIQUEIDENTIFIER,
        sessionStartCash FLOAT,
        payOrders FLOAT,
        sumWriteoffOrders FLOAT,
        salesCash FLOAT,
        salesCredit FLOAT,
        salesCard FLOAT,
        payIn FLOAT,
        payOut FLOAT,
        payIncome FLOAT,
        cashRemain FLOAT,
        cashDiff FLOAT,
        sessionStatus NVARCHAR(50),
        conceptionId UNIQUEIDENTIFIER,
        pointOfSaleId UNIQUEIDENTIFIER
    )
END
""")
conn.commit()

# === ЗАГРУЗКА ДАННЫХ ===
for _, row in df.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dbo.iiko_cash_shifts_fact WHERE id = ?)
        INSERT INTO dbo.iiko_cash_shifts_fact (
            id, sessionNumber, fiscalNumber, cashRegNumber, cashRegSerial,
            openDate, closeDate, acceptDate, managerId, responsibleUserId,
            sessionStartCash, payOrders, sumWriteoffOrders, salesCash, salesCredit,
            salesCard, payIn, payOut, payIncome, cashRemain, cashDiff,
            sessionStatus, conceptionId, pointOfSaleId
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        row.get("id"),
        row.get("id"),
        row.get("sessionNumber"),
        row.get("fiscalNumber"),
        row.get("cashRegNumber"),
        row.get("cashRegSerial"),
        row.get("openDate"),
        row.get("closeDate"),
        row.get("acceptDate"),
        row.get("managerId"),
        row.get("responsibleUser"),
        safe_float(row.get("sessionStartCash")),
        safe_float(row.get("payOrders")),
        safe_float(row.get("sumWriteoffOrders")),
        safe_float(row.get("salesCash")),
        safe_float(row.get("salesCredit")),
        safe_float(row.get("salesCard")),
        safe_float(row.get("payIn")),
        safe_float(row.get("payOut")),
        safe_float(row.get("payIncome")),
        safe_float(row.get("cashRemain")),
        safe_float(row.get("cashDiff")),
        row.get("sessionStatus"),
        row.get("conception"),
        row.get("pointOfSale")
    )

conn.commit()
conn.close()

print("✅ Данные успешно загружены в SQL Server.")
