import requests
import pandas as pd

# --- Auth ---
base_url =  "https://roma-pizza-co.iiko.it/resto"  # adjust as needed
login = "Admin3"
password = "05962ad33b64478ff569e9c75509d66a623b0537"
auth_url = f"{base_url}/api/auth?login={login}&pass={password}"

auth_response = requests.get(auth_url)
token = auth_response.text.strip()  # ✅ this fixes the error
print("Token received:", token)