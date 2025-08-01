from iiko_auth import get_iiko_token
from conn_db import get_connection

from accaunts import load_accaunts
from attendance_types import load_attendance_types
from attendances import load_attendances
from balance_report import load_balance_stores
from delivery_consolidated import load_delivery_consolidated
from delivery_couriers import load_delivery_couriers
from delivery_halfHourDetailed import load_delivery_halfHour
from delivery_loyalty import load_delivery_loyalty
from employee_availability import load_employee_availability
from employee_roles import load_employee_roles
from employee_shifts import load_employee_shifts
from employees_salary import load_employees_salary
from employees import load_employees
from events import load_events
from cash_shifts import load_cash_shifts
from payment_types import load_payment_types
from priceCategories import load_price_categories
from product_categories import load_product_categories
from product_group import load_prduct_group
from product_types import load_product_types
from products import load_products
from replications import load_replications
from balance_counteragents import load_balance_contragents
from schedule_types import load_schedule_types
from stores import load_stores
from storeOperations import load_store_operations
from storeReportPresets import load_store_report_presets
from suppliers import load_supliers

def main():
    # ✅ Получаем токен и подключение
    token = get_iiko_token()
    conn = get_connection()

    try:
        # ✅ Передаём token и conn в функции
        load_accaunts(token, conn)
        load_attendance_types(token, conn)
        load_attendances(token, conn)
        load_balance_stores(token, conn)
        load_delivery_consolidated(token, conn)
        load_delivery_couriers(token, conn)
        load_delivery_halfHour(token, conn)
        load_delivery_loyalty(token, conn)
        load_employee_availability(token, conn)
        load_employee_roles(token, conn)
        load_employee_shifts(token, conn)
        load_employees_salary(token, conn)
        load_employees(token, conn)
        load_events(token, conn)
        load_cash_shifts(token, conn)
        load_payment_types(token, conn)
        load_price_categories(token, conn)
        load_product_categories(token, conn)
        load_prduct_group(token, conn)
        load_product_types(token, conn)
        load_products(token, conn)
        load_replications(token, conn)
        load_balance_contragents(token, conn)
        load_schedule_types(token, conn)
        load_stores(token, conn)
        load_store_operations(token, conn)
        load_store_report_presets(token, conn)
        load_supliers(token, conn)

    finally:
        # ✅ Закрываем подключение к БД
        conn.close()
        print("✅ Подключение к БД закрыто")

if __name__ == "__main__":
    main()
