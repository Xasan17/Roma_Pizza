from iiko_auth import get_iiko_token
from suppliers import load_supliers
from accaunts import load_accaunts

# и так далее для других таблиц...

# ✅ Получаем токен один раз
token = get_iiko_token()

# ✅ Передаём токен в каждую функцию
load_supliers(token)
load_accaunts(token)