# NEED improve 
# berikan filter tambahan sebagai berikut
# bill_statements.bill_amount >= 0

# NICE TO HAVE
# pisahkan string query menjadi variable berisi string kedalam folder /scripts/query/(ex:build_credit_client_table.py, etc...)

from resources.utils.db_conn import conn
from resources.scripts.query.build_clients_table import build_sql as clients
from resources.scripts.query.build_payment_history_table import build_sql as payment_history
from resources.scripts.query.build_bill_statements_table import build_sql as bill_statements
from resources.scripts.query.build_payments_table import build_sql as payments

def main():
    queries = [clients(), payment_history(), bill_statements(), payments()]


    try:
        with conn.cursor() as cur:
            for q in queries:
                print("Running query...")
                cur.execute(q)
            conn.commit()
            print("All queries executed successfully ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()
