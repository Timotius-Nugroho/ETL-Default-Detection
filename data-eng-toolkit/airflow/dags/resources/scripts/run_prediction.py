import pandas as pd
import pickle
import psycopg2
from pathlib import Path
from resources.utils.db_conn import conn
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mendapatkan direktori root dari DAG
dag_root = Path(__file__).parent.parent.parent
model_path = dag_root / "resources" / "models" / "credit_card_default_model_v2.pkl"

# 1. Load model
try:
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    logger.info("✅ Model berhasil dimuat")
except FileNotFoundError:
    logger.error("❌ Model file tidak ditemukan")
    raise
except Exception as e:
    logger.error(f"❌ Error saat memuat model: {e}")
    raise

def main():
    cursor = None
    try:
        # 2. Buat cursor
        cursor = conn.cursor()
        
        # 3. Query data
        logger.info("📊 Mengambil data dari database...")
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
        logger.info(f"✅ Berhasil mengambil {len(df)} records")

        if df.empty:
            logger.warning("⚠️ Tidak ada data untuk diprediksi")
            return

        # 4. Drop kolom ID & target
        X = df.drop(columns=["customer_id", "default_payment_next_month"])
        
        # 5. Prediksi
        logger.info("🤖 Melakukan prediksi...")
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)[:, 1]
        logger.info("✅ Prediksi selesai")

        # 6. Prepare batch update data
        tmp_updates = []
        gold_updates = []
        
        for idx, row in df.iterrows():
            prob_default = bool(probabilities[idx] > 0.5)    
            customer_id = row['customer_id']
            
            tmp_updates.append((prob_default, customer_id))
            gold_updates.append((prob_default, customer_id))

        # 7. Batch update ke database
        logger.info("💾 Melakukan batch update ke database...")
        
        # Update ke tmp_ml_pred.credit_card_default
        update_query_tmp = """
            UPDATE tmp_ml_pred.credit_card_default
            SET default_payment_next_month = %s
            WHERE id = %s
        """
        cursor.executemany(update_query_tmp, tmp_updates)
        logger.info(f"✅ Updated {cursor.rowcount} records di tmp_ml_pred.credit_card_default")

        # Update ke gold.fact_credit
        update_query_gold = """
            UPDATE gold.fact_credit
            SET default_flag = %s
            WHERE client_id = %s
        """
        cursor.executemany(update_query_gold, gold_updates)
        logger.info(f"✅ Updated {cursor.rowcount} records di gold.fact_credit")

        # Commit perubahan
        conn.commit()
        logger.info("✅ Semua perubahan berhasil di-commit")
        
        # Log summary statistik
        total_predictions = len(predictions)
        default_predictions = sum(predictions)
        default_rate = (default_predictions / total_predictions) * 100
        
        logger.info(f"📈 Summary Prediksi:")
        logger.info(f"   Total records: {total_predictions}")
        logger.info(f"   Predicted defaults: {default_predictions}")
        logger.info(f"   Default rate: {default_rate:.2f}%")

    except psycopg2.Error as db_error:
        logger.error(f"❌ Database error: {db_error}")
        if conn:
            conn.rollback()
            logger.info("🔄 Database rollback dilakukan")
        raise
        
    except Exception as e:
        logger.error(f"❌ Error during prediction: {e}")
        if conn:
            conn.rollback()
            logger.info("🔄 Database rollback dilakukan")
        raise
        
    finally:
        # Pastikan cursor dan connection ditutup dengan benar
        if cursor:
            cursor.close()
            logger.info("🔒 Cursor ditutup")
        if conn:
            conn.close()
            logger.info("🔒 Connection ditutup")

if __name__ == "__main__":
    main()