# file: resources/prepare_prediction_dataset.py

from resources.utils.db_conn import conn

def main():
    query = """
    INSERT INTO tmp_ml_pred.credit_card_default (
        id,
        limit_bal,
        sex,
        education,
        marriage,
        age,
        pay_0,
        pay_2,
        pay_3,
        pay_4,
        pay_5,
        pay_6,
        bill_amt1,
        bill_amt2,
        bill_amt3,
        bill_amt4,
        bill_amt5,
        bill_amt6,
        pay_amt1,
        pay_amt2,
        pay_amt3,
        pay_amt4,
        pay_amt5,
        pay_amt6,
        default_payment_next_month
    )
    SELECT
        c.client_id AS id,
        c.limit_bal,
        s.sex_desc AS sex,
        e.education_desc AS education,
        m.marriage_desc AS marriage,
        c.age,

        -- pay status
        MAX(CASE WHEN f.rn = 1 THEN f.pay_status END) AS pay_0,
        MAX(CASE WHEN f.rn = 2 THEN f.pay_status END) AS pay_2,
        MAX(CASE WHEN f.rn = 3 THEN f.pay_status END) AS pay_3,
        MAX(CASE WHEN f.rn = 4 THEN f.pay_status END) AS pay_4,
        MAX(CASE WHEN f.rn = 5 THEN f.pay_status END) AS pay_5,
        MAX(CASE WHEN f.rn = 6 THEN f.pay_status END) AS pay_6,

        -- bill amount
        MAX(CASE WHEN f.rn = 1 THEN f.bill_amount END) AS bill_amt1,
        MAX(CASE WHEN f.rn = 2 THEN f.bill_amount END) AS bill_amt2,
        MAX(CASE WHEN f.rn = 3 THEN f.bill_amount END) AS bill_amt3,
        MAX(CASE WHEN f.rn = 4 THEN f.bill_amount END) AS bill_amt4,
        MAX(CASE WHEN f.rn = 5 THEN f.bill_amount END) AS bill_amt5,
        MAX(CASE WHEN f.rn = 6 THEN f.bill_amount END) AS bill_amt6,

        -- payment amount
        MAX(CASE WHEN f.rn = 1 THEN f.payment_amount END) AS pay_amt1,
        MAX(CASE WHEN f.rn = 2 THEN f.payment_amount END) AS pay_amt2,
        MAX(CASE WHEN f.rn = 3 THEN f.payment_amount END) AS pay_amt3,
        MAX(CASE WHEN f.rn = 4 THEN f.payment_amount END) AS pay_amt4,
        MAX(CASE WHEN f.rn = 5 THEN f.payment_amount END) AS pay_amt5,
        MAX(CASE WHEN f.rn = 6 THEN f.payment_amount END) AS pay_amt6,

        FALSE AS default_payment_next_month
    FROM (
        SELECT
            fc.*,
            ROW_NUMBER() OVER (PARTITION BY fc.client_id ORDER BY dt.year DESC, dt.month DESC) AS rn
        FROM gold.fact_credit fc
        JOIN gold.dim_time dt ON fc.time_id = dt.time_id
    ) f
    JOIN gold.dim_client c ON f.client_id = c.client_id
    LEFT JOIN gold.dim_sex s ON c.sex_id = s.sex_id
    LEFT JOIN gold.dim_education e ON c.education_id = e.education_id
    LEFT JOIN gold.dim_marriage m ON c.marriage_id = m.marriage_id
    WHERE f.rn <= 6
    GROUP BY
        c.client_id, c.limit_bal, s.sex_desc, e.education_desc, m.marriage_desc, c.age;
    """

    try:
        with conn.cursor() as cur:
            print("Running query to populate tmp_ml_pred.credit_card_default ...")
            cur.execute(query)
            conn.commit()
            print("Prediction dataset populated successfully ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()
