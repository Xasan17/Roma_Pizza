import sys
import os
from attendances_all import load_attendances_all
from balance_counteragents_all import load_balance_counteragents_all
from cash_shifts_all import load_cash_shifts_all
from delivery_consolidated_all import load_delivery_consolidated_all
from delivery_couriers_all import load_delivery_couriers_all
from delivery_halfHourDetailed_all import load_delivery_halfHourDetailed_all
from delivery_loyality_all import load_delivery_loyality_all
from delivery_order_cycle import load_delivery_order_cycle
from store_balance_history_loader import load_store_balance_history_loader
from storeOperations_all import load_storeOperations_all
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from conn_db import get_connection

def main():
    # ✅ Получаем токен и подключение
    conn = get_connection()

    try:
        load_store_balance_history_loader(conn)
        load_attendances_all(conn)
        load_balance_counteragents_all(conn)
        load_cash_shifts_all(conn)
        load_delivery_consolidated_all(conn)
        load_delivery_couriers_all(conn)
        load_delivery_halfHourDetailed_all(conn)
        load_delivery_loyality_all(conn)
        load_delivery_order_cycle(conn)
        load_storeOperations_all(conn)
    finally:
        conn.close()
        print("✅ Подключение к БД закрыто")

if __name__ == "__main__":
    main()