import requests
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings()

# === НАСТРОЙКИ ===
token = "8063ebd3-dc30-1412-4660-aff906b8b6cd"
correct_store_id = "BFDF7F94-1B37-4AC9-BA0E-568B562C6F0A"  # ✅ Реальный ID склада
incorrect_store_id = "1239d270-1bbe-f64f-b7ea-5f00518ef508"  # ❌ Проблемный ID

# === ЗАГРУЗКА И ИЗМЕНЕНИЕ XML ===
tree = ET.parse("invoice.xml")
root = tree.getroot()

# Заменим неправильный ID на правильный
for elem in root.iter():
    if elem.tag in ["store", "defaultStore"]:
        if elem.text == incorrect_store_id:
            elem.text = correct_store_id

# Сохраняем исправленный XML
tree.write("invoice_fixed.xml", encoding="utf-8", xml_declaration=True)

# === ОТПРАВКА POST ЗАПРОСА В IIKO ===
url = "https://roma-pizza-co.iiko.it/resto/api/documents/import/incomingInvoice"
params = {
    "key": token
}

# Загружаем изменённый XML-файл
with open("invoice_fixed.xml", "rb") as f:
    xml_data = f.read()

headers = {
    "Content-Type": "application/xml"
}

response = requests.post(url, params=params, headers=headers, data=xml_data, verify=False)

# === ОБРАБОТКА ОТВЕТА ===
if response.ok:
    print("✅ Запрос успешно отправлен")
    print(response.text)
else:
    print(f"❌ Ошибка {response.status_code}:")
    print(response.text)
