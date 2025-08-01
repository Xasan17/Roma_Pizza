import requests
import urllib3

urllib3.disable_warnings()

def get_iiko_token():
    base_url = "https://roma-pizza-co.iiko.it/resto"
    login = "Admin3"
    password = "05962ad33b64478ff569e9c75509d66a623b0537"
    auth_url = f"{base_url}/api/auth?login={login}&pass={password}"

    response = requests.get(auth_url, verify=False)  # <- отключаем проверку SSL
    if response.ok:
        return response.text.strip()
    else:
        raise Exception(f"❌ Ошибка авторизации: {response.status_code} — {response.text}")