import requests
import pandas as pd
import pyodbc
import urllib3

# Отключаем SSL warning
urllib3.disable_warnings()

# 🔑 API ключ
token = "361267c8-bb77-efd1-f09f-6c172939b7a5"
base_url = "https://roma-pizza-co.iiko.it/resto/api"
headers = {"Content-Type": "application/json"}

# 📦 Функция безопасного преобразования
def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# 📥 Получение данных из API
charts_url = f"{base_url}/v2/assemblyCharts/getAll"
response = requests.get(charts_url, params={"key": token}, headers=headers, verify=False)
data = response.json()
charts = data.get("assemblyCharts", [])

# 📊 Разделяем на две таблицы: рецепты и ингредиенты
chart_rows = []
item_rows = []

for chart in charts:
    chart_row = {
        "chart_id": chart.get("id"),
        "assembled_product_id": chart.get("assembledProductId"),
        "date_from": chart.get("dateFrom"),
        "date_to": chart.get("dateTo"),
        "assembled_amount": safe_float(chart.get("assembledAmount")),
        "writeoff_strategy": chart.get("productWriteoffStrategy"),
        "size_strategy": chart.get("productSizeAssemblyStrategy"),
        "technology_description": chart.get("technologyDescription"),
        "description": chart.get("description"),
        "appearance": chart.get("appearance"),
        "organoleptic": chart.get("organoleptic"),
        "output_comment": chart.get("outputComment")
    }
    chart_rows.append(chart_row)

    for item in chart.get("items", []):
        item_row = {
            "chart_id": chart.get("id"),
            "item_id": item.get("id"),
            "sort_weight": safe_float(item.get("sortWeight")),
            "product_id": item.get("productId"),
            "product_size_spec": item.get("productSizeSpecification"),
            "amount_in": safe_float(item.get("amountIn")),
            "amount_middle": safe_float(item.get("amountMiddle")),
            "amount_out": safe_float(item.get("amountOut")),
            "amount_in1": safe_float(item.get("amountIn1")),
            "amount_out1": safe_float(item.get("amountOut1")),
            "amount_in2": safe_float(item.get("amountIn2")),
            "amount_out2": safe_float(item.get("amountOut2")),
            "amount_in3": safe_float(item.get("amountIn3")),
            "amount_out3": safe_float(item.get("amountOut3")),
            "package_count": safe_float(item.get("packageCount")),
            "package_type_id": item.get("packageTypeId")
        }
        item_rows.append(item_row)

df_charts = pd.DataFrame(chart_rows)
df_items = pd.DataFrame(item_rows)

# 📡 Подключение к SQL Server
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;DATABASE=YourDatabaseName;UID=your_username;PWD=your_password"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 🛠️ Создание таблиц, если они не существуют
cursor.execute("""
IF OBJECT_ID('dbo.stagging_iiko_assembly_charts', 'U') IS NULL
CREATE TABLE dbo.stagging_iiko_assembly_charts (
    chart_id UNIQUEIDENTIFIER PRIMARY KEY,
    assembled_product_id UNIQUEIDENTIFIER,
    date_from DATE,
    date_to DATE,
    assembled_amount FLOAT,
    writeoff_strategy NVARCHAR(50),
    size_strategy NVARCHAR(50),
    technology_description NVARCHAR(MAX),
    description NVARCHAR(MAX),
    appearance NVARCHAR(MAX),
    organoleptic NVARCHAR(MAX),
    output_comment NVARCHAR(MAX)
)
""")

cursor.execute("""
IF OBJECT_ID('dbo.stagging_iiko_assembly_chart_items', 'U') IS NULL
CREATE TABLE dbo.stagging_iiko_assembly_chart_items (
    item_id UNIQUEIDENTIFIER PRIMARY KEY,
    chart_id UNIQUEIDENTIFIER,
    sort_weight FLOAT,
    product_id UNIQUEIDENTIFIER,
    product_size_spec UNIQUEIDENTIFIER,
    amount_in FLOAT,
    amount_middle FLOAT,
    amount_out FLOAT,
    amount_in1 FLOAT,
    amount_out1 FLOAT,
    amount_in2 FLOAT,
    amount_out2 FLOAT,
    amount_in3 FLOAT,
    amount_out3 FLOAT,
    package_count FLOAT,
    package_type_id UNIQUEIDENTIFIER,
    FOREIGN KEY (chart_id) REFERENCES dbo.stagging_iiko_assembly_charts(chart_id)
)
""")
conn.commit()

# 🧹 Очистка таблиц перед загрузкой
cursor.execute("DELETE FROM dbo.stagging_iiko_assembly_chart_items")
cursor.execute("DELETE FROM dbo.stagging_iiko_assembly_charts")
conn.commit()

# 📤 Загрузка данных в таблицу рецептов
for _, row in df_charts.iterrows():
    cursor.execute("""
        INSERT INTO dbo.stagging_iiko_assembly_charts (
            chart_id, assembled_product_id, date_from, date_to,
            assembled_amount, writeoff_strategy, size_strategy,
            technology_description, description, appearance,
            organoleptic, output_comment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    row["chart_id"], row["assembled_product_id"], row["date_from"], row["date_to"],
    row["assembled_amount"], row["writeoff_strategy"], row["size_strategy"],
    row["technology_description"], row["description"], row["appearance"],
    row["organoleptic"], row["output_comment"])

# 📤 Загрузка данных в таблицу ингредиентов
for _, row in df_items.iterrows():
    cursor.execute("""
        INSERT INTO dbo.stagging_iiko_assembly_chart_items (
            item_id, chart_id, sort_weight, product_id, product_size_spec,
            amount_in, amount_middle, amount_out,
            amount_in1, amount_out1, amount_in2, amount_out2,
            amount_in3, amount_out3, package_count, package_type_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    row["item_id"], row["chart_id"], row["sort_weight"], row["product_id"], row["product_size_spec"],
    row["amount_in"], row["amount_middle"], row["amount_out"],
    row["amount_in1"], row["amount_out1"], row["amount_in2"], row["amount_out2"],
    row["amount_in3"], row["amount_out3"], row["package_count"], row["package_type_id"])

# ✅ Финал
conn.commit()
cursor.close()
conn.close()
print("✅ Данные успешно загружены в SQL Server.")
