import requests
import urllib3
import pandas as pd
urllib3.disable_warnings()  # чтобы отключить InsecureRequestWarning

# Твой токен
token = "9f90f3ff-9f40-5202-d65f-0457813ea15b"

# Дата и время (обязательно в формате ISO 8601)
timestamp = "2025-07-09T12:10:10"

# Формируем URL запроса
url = f"https://roma-pizza-co.iiko.it/resto/api/v2/reports/balance/stores"
params = {
    "key": token,
    "timestamp": timestamp
}

# Отправляем GET-запрос
response = requests.get(url, params=params, verify=False)

# Проверка и вывод
if response.ok:
    data = response.json()
else:
    print("[!] Ошибка:", response.status_code, response.text)
df =pd.DataFrame(data)
df.to_excel("iiko_balance.xlsx", index=False)
print("✅ Данные сохранены в iiko_balance.xlsx")