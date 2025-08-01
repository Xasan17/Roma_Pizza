import requests
import pandas as pd
import pyodbc
from datetime import datetime
import urllib3

# 🔇 Отключить предупреждения по HTTPS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def load_cash_shifts(token,conn):
    base_url = "https://roma-pizza-co.iiko.it"
    url = f"{base_url}/resto/api/v2/cashshifts/list"

    # 📅 Сегодняшняя дата в формате YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")

    params = {
        "key": token,
        "openDateFrom": today,
        "openDateTo": today,
        "status": "ANY"
    }

    # 📥 Запрос к API
    response = requests.get(url, params=params, verify=False)
    if response.status_code != 200:
        print(f"❌ Ошибка запроса: {response.status_code} - {response.text}")
        exit()

    try:
        data = response.json()
    except Exception as e:
        print("❌ Ошибка парсинга JSON:", e)
        exit()

    # 🧾 Загрузка в DataFrame
    df = pd.DataFrame(data)

    # 🧾 Структура ожидаемых колонок
    expected_columns = [
        "id", "sessionNumber", "fiscalNumber", "cashRegNumber", "cashRegSerial",
        "openDate", "closeDate", "acceptDate", "managerId", "responsibleUserId",
        "sessionStartCash", "payOrders", "sumWriteoffOrders",
        "salesCash", "salesCredit", "salesCard",
        "payIn", "payOut", "payIncome", "cashRemain", "cashDiff",
        "sessionStatus", "conceptionId", "pointOfSaleId"
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    float_cols = [
        "sessionStartCash", "payOrders", "sumWriteoffOrders", "salesCash",
        "salesCredit", "salesCard", "payIn", "payOut", "payIncome",
        "cashRemain", "cashDiff"
    ]
    datetime_cols = ["openDate", "closeDate", "acceptDate"]

    # 💡 Преобразование чисел
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # 💡 Очистка и форматирование дат
    def clean_datetime(value):
        try:
            if pd.isnull(value) or value in ["", "0001-01-01T00:00:00", "0001-01-01T00:00:00Z"]:
                return None
            dt = pd.to_datetime(value, errors='coerce')
            if pd.isnull(dt) or dt.year < 1753 or dt.year > 9999:
                return None
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except:
            return None

    for col in datetime_cols:
        df[col] = df[col].apply(clean_datetime)

    # 🛠 Подключение к SQL Server

    try:
        cursor = conn.cursor()
    except Exception as e:
        print("❌ Ошибка подключения к SQL Server:", e)
        exit()

    # ⚠️ Удаление данных за текущий день
    try:
        delete_sql = """
        DELETE FROM iiko_cash_shifts_fact
        WHERE CAST(openDate AS DATE) = ?
        """
        cursor.execute(delete_sql, today)
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка при удалении данных за {today}: {e}")
        conn.close()
        exit()

    # ✅ SQL-запрос вставки
    insert_sql = """
    INSERT INTO iiko_cash_shifts_fact (
        id, sessionNumber, fiscalNumber, cashRegNumber, cashRegSerial,
        openDate, closeDate, acceptDate, managerId, responsibleUserId,
        sessionStartCash, payOrders, sumWriteoffOrders,
        salesCash, salesCredit, salesCard,
        payIn, payOut, payIncome, cashRemain, cashDiff,
        sessionStatus, conceptionId, pointOfSaleId
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # 🔄 Вставка данных
    errors = 0
    inserted = 0

    for index, row in df.iterrows():
        try:
            cursor.execute(insert_sql, (
                row['id'],
                int(row['sessionNumber']) if pd.notnull(row['sessionNumber']) else None,
                int(row['fiscalNumber']) if pd.notnull(row['fiscalNumber']) else None,
                int(row['cashRegNumber']) if pd.notnull(row['cashRegNumber']) else None,
                row['cashRegSerial'],
                row['openDate'], row['closeDate'], row['acceptDate'],
                row['managerId'], row['responsibleUserId'],
                row['sessionStartCash'], row['payOrders'], row['sumWriteoffOrders'],
                row['salesCash'], row['salesCredit'], row['salesCard'],
                row['payIn'], row['payOut'], row['payIncome'],
                row['cashRemain'], row['cashDiff'],
                row['sessionStatus'], row['conceptionId'], row['pointOfSaleId']
            ))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Ошибка при вставке строки {index}: {e}")
            errors += 1

    conn.commit()
    cursor.close()

    print(f"✅ Загрузка завершена. Вставлено: {inserted}, ошибок: {errors}")
