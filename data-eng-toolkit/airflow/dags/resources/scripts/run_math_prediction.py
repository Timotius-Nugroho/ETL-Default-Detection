from resources.utils.db_conn import conn

def main():
    queries = [
        # 1) Transfer Clients (cleansing age >= 18)
        """
        UPDATE gold.fact_credit fc
        SET default_flag = CASE 
            WHEN fc.pay_status > 0 OR (fc.payment_amount < (fc.bill_amount * 0.3)) 
            THEN TRUE 
            ELSE FALSE 
        END;
        """,
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
