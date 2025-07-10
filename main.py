import requests
import pandas as pd
import urllib3

# 🔕 Отключаем предупреждения HTTPS
urllib3.disable_warnings()

# 🔐 Токен и URL
token = "5f816fa6-9590-d907-cc39-2a9fd26853ec"
url = "https://roma-pizza-co.iiko.it/resto/api/v2/entities/products/list"
params = {
    "key": token,
    "includeDeleted": "false"
}

# 🔍 Отправляем GET-запрос
response = requests.get(url, params=params, verify=False)

if response.ok:
    try:
        data = response.json()
        df = pd.json_normalize(data)

        # 💾 Сохраняем в Excel
        df.to_excel("iiko_products.xlsx", index=False)
        print("✅ Номенклатура сохранена в iiko_products.xlsx")
    except Exception as e:
        print("❌ Ошибка при разборе JSON:", e)
else:
    print(f"[!] Ошибка {response.status_code}: {response.text}")
