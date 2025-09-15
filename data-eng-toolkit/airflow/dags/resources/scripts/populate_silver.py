# NEED improve 
# berikan filter tambahan sebagai berikut
# bill_statements.bill_amount >= 0

# NICE TO HAVE
# pisahkan string query menjadi variable berisi string kedalam folder /scripts/query/(ex:build_credit_client_table.py, etc...)

from resources.utils.db_conn import conn

def main():
    queries = [
        # 1) Transfer Clients (cleansing age >= 18)
        """
        INSERT INTO silver.clients (client_id, limit_bal, sex, education, marriage, age)
        SELECT client_id, limit_bal, sex, education, marriage, age
        FROM external.clients
        WHERE age >= 18;
        """,

        # 2) Transfer Payment History
        """
        INSERT INTO silver.payment_history (id, client_id, month, pay_status)
        SELECT ph.id, ph.client_id, ph.month, ph.pay_status
        FROM external.payment_history ph
        JOIN external.clients c ON ph.client_id = c.client_id
        WHERE c.age >= 18;
        """,

        # 3) Transfer Bill Statements
        """
        INSERT INTO silver.bill_statements (id, client_id, month, bill_amount)
        SELECT bs.id, bs.client_id, bs.month, bs.bill_amount
        FROM external.bill_statements bs
        JOIN external.clients c ON bs.client_id = c.client_id
        WHERE c.age >= 18;
        """,

        # 4) Transfer Payments
        """
        INSERT INTO silver.payments (id, client_id, month, payment_amount)
        SELECT p.id, p.client_id, p.month, p.payment_amount
        FROM external.payments p
        JOIN external.clients c ON p.client_id = c.client_id
        WHERE c.age >= 18;
        """
    ]

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
