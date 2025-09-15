from resources.utils.db_conn import conn

def main():
    queries = [
        # Silver
        "TRUNCATE TABLE silver.clients CASCADE;",
        "TRUNCATE TABLE silver.payment_history CASCADE;",
        "TRUNCATE TABLE silver.bill_statements CASCADE;",
        "TRUNCATE TABLE silver.payments CASCADE;",

        # Gold
        "TRUNCATE TABLE gold.dim_sex CASCADE;",
        "TRUNCATE TABLE gold.dim_education CASCADE;",
        "TRUNCATE TABLE gold.dim_marriage CASCADE;",
        "TRUNCATE TABLE gold.dim_client CASCADE;",
        "TRUNCATE TABLE gold.dim_time CASCADE;",
        "TRUNCATE TABLE gold.fact_credit CASCADE;",

        # Tmp prediction
        "TRUNCATE TABLE tmp_ml_pred.credit_card_default CASCADE;"
    ]

    try:
        with conn.cursor() as cur:
            for q in queries:
                print(f"Executing: {q}")
                cur.execute(q)
            conn.commit()
            print("✅ Cleanup completed, all tables truncated.")
    except Exception as e:
        conn.rollback()
        print("❌ Error during cleanup:", e)
    finally:
        conn.close()
