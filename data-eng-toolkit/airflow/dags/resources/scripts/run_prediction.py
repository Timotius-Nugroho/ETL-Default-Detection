# NEED TO DO:
# install lib pip yg dibutuhakn dengan venv pada source venv/bin/activate
# fetch dataset pada tmp_ml_pred.credit_card_default
# lakukan predict dengan model, lalu ouput true/false 
# akan disimpan pada kolom gold.fact_credit.default_flag berdasarkan client_id terkait


import pandas as pd
# import joblib
from resources.utils.db_conn import conn

def main():
    try:
        # 1. Load model
        # model = joblib.load("model.pkl")  # pastikan file model.pkl ada

        # 2. Ambil dataset dari DB
        query = "SELECT * FROM tmp_ml_pred.credit_card_default;"
        df = pd.read_sql(query, conn)

        # Drop kolom id & default_payment_next_month dari fitur
        features = df.drop(columns=["id", "default_payment_next_month"])

        # 3. Lakukan prediksi
        # predictions = model.predict(features)

        # 4. Simpan hasil ke DB
        insert_query = """
            INSERT INTO tmp_ml_pred.prediction_result (id, prediction)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE SET prediction = EXCLUDED.prediction;
        """

        # with conn.cursor() as cur:
        #     for client_id, pred in zip(df["id"], predictions):
        #         cur.execute(insert_query, (client_id, bool(pred)))
        #     conn.commit()

        print("Prediction completed and saved ✅")

    except Exception as e:
        conn.rollback()
        print("Error during prediction ❌:", e)
    finally:
        conn.close()
