import requests
import pandas as pd

# === НАСТРОЙКИ ===
BASE_URL = "http://your-iiko-server-address/resto/api"
LOGIN = "your_login"
PASSWORD = "your_password"


def authenticate():
    url = f"{BASE_URL}/auth"
    payload = {
        "login": 'Admin3',
        "pass": '456321'
    }
    response = requests.post(url, json=payload)
    if response.ok:
        session_key = response.text.strip('"')  # Ответ — строка в кавычках
        print("[✓] Авторизация успешна")
        return session_key
    else:
        raise Exception(f"[!] Ошибка авторизации: {response.status_code} {response.text}")


def get_organizations(session_key):
    url = f"{BASE_URL}/organizations"
    cookies = {'key': session_key}
    response = requests.get(url, cookies=cookies)
    if response.ok:
        orgs = response.json()
        df_orgs = pd.DataFrame(orgs)
        df_orgs.to_csv("organizations.csv", index=False, encoding='utf-8-sig')
        print(f"[✓] Организации загружены: {len(df_orgs)}")
        return df_orgs
    else:
        raise Exception(f"[!] Ошибка при получении организаций: {response.status_code} {response.text}")


def get_nomenclature(session_key, organization_id):
    url = f"{BASE_URL}/v2/nomenclature"
    params = {'organization': organization_id}
    cookies = {'key': session_key}
    response = requests.get(url, params=params, cookies=cookies)
    if response.ok:
        data = response.json()
        products = data.get("products", [])
        df_products = pd.json_normalize(products)
        df_products.to_csv("nomenclature.csv", index=False, encoding='utf-8-sig')
        print(f"[✓] Номенклатура загружена: {len(df_products)}")
    else:
        raise Exception(f"[!] Ошибка при получении номенклатуры: {response.status_code} {response.text}")


# === ОСНОВНОЙ БЛОК ===
if __name__ == "__main__":
    try:
        session_key = authenticate()
        df_orgs = get_organizations(session_key)

        if not df_orgs.empty:
            first_org_id = df_orgs.iloc[0]['id']
            get_nomenclature(session_key, first_org_id)
        else:
            print("[!] Нет доступных организаций")
    except Exception as e:
        print(e)
