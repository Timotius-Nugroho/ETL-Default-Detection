import pandas as pd
import pickle
import psycopg2
from pathlib import Path
from resources.utils.db_conn import conn

import os
   
# Mendapatkan direktori root dari DAG
dag_root = Path(__file__).parent.parent.parent
model_path = dag_root / "resources" / "models" / "credit_card_default_model_v2.pkl"

# 1. Load model
with open(model_path, "rb") as f:
    model = pickle.load(f)

def main():
    try:
        # 3. Query data
        query = """
            SELECT 
                id as customer_id,
                limit_bal, sex, education, marriage, age,
                pay_0, pay_2, pay_3, pay_4, pay_5, pay_6,
                bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6,
                pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6,
                default_payment_next_month
            FROM tmp_ml_pred.credit_card_default
        """
        df = pd.read_sql(query, conn)

        # 4. Drop kolom ID & target
        X = df.drop(columns=["customer_id", "default_payment_next_month"])

        # 5. Prediksi
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)[:,1]

        # 6. Update hasil ke database
        update_query_tmp = """
            UPDATE tmp_ml_pred.credit_card_default
            SET default_payment_next_month = %s
            WHERE id = %s
        """

        update_query_gold = """
            UPDATE gold.fact_credit
            SET default_flag = %s
            WHERE client_id = %s
        """

        for idx, row in df.iterrows():
            prob_default = bool(probabilities[idx] > 0.5)    
            customer_id = row['customer_id']


            # update ke tmp_ml_pred.credit_card_default
            cursor.execute(update_query_tmp, (prob_default, customer_id))

            # update ke gold.fact_credit
            cursor.execute(update_query_gold, (prob_default, customer_id))

        # commit perubahan
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        conn.rollback()
        print("Error during prediction ❌:", e)
    finally:
        conn.close()
