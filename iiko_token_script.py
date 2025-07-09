import requests
import urllib3

# 🛡️ Отключаем предупреждения о небезопасном SSL-сертификате (если он самоподписан)
urllib3.disable_warnings()

# 🔧 Настройки
login = "Admin3"
password_sha1 = "05962ad33b64478ff569e9c75509d66a623b0537"
BASE_URL = "https://roma-pizza-co.iiko.it/resto"

# 🔐 Авторизация
auth_url = f"{BASE_URL}/api/auth?login={login}&pass={password_sha1}"
auth_response = requests.get(auth_url, verify=False)

if auth_response.ok:
    token = auth_response.text.strip('"')
    print("[✓] Токен получен:", token)

    # 🧾 Получение общей информации (организации, пользователи, версия и т.д.)
    info_url = f"{BASE_URL}/api/info?key={token}"
    info_response = requests.get(info_url, verify=False)

    if info_response.ok:
        info_data = info_response.json()
        print("[✓] Информация о системе получена:")

        # Печатаем полученную информацию красиво
        from pprint import pprint
        pprint(info_data)

        # Дополнительно — если хочешь извлечь ID организации:
        if "organizations" in info_data:
            print("\nОрганизации:")
            for org in info_data["organizations"]:
                print(f"- {org['name']} (ID: {org['id']})")
    else:
        print("[!] Ошибка при запросе /info:", info_response.status_code, info_response.text)

else:
    print("[!] Ошибка авторизации:", auth_response.status_code, auth_response.text)
