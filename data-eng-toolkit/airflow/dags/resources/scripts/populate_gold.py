from resources.utils.db_conn import conn

def main():
    queries = [
        # 1. Populate dim_sex
        """
        INSERT INTO gold.dim_sex (sex_id, sex_desc)
        SELECT DISTINCT s.sex, 
               CASE s.sex
                 WHEN 1 THEN 'Male'
                 WHEN 2 THEN 'Female'
                 ELSE 'Unknown'
               END
        FROM silver.clients s;
        """,

        # 2. Populate dim_education
        """
        INSERT INTO gold.dim_education (education_id, education_desc)
        SELECT DISTINCT s.education,
               CASE s.education
                 WHEN 1 THEN 'Graduate School'
                 WHEN 2 THEN 'University'
                 WHEN 3 THEN 'High School'
                 WHEN 4 THEN 'Others'
                 ELSE 'Unknown'
               END
        FROM silver.clients s;
        """,

        # 3. Populate dim_marriage
        """
        INSERT INTO gold.dim_marriage (marriage_id, marriage_desc)
        SELECT DISTINCT s.marriage,
               CASE s.marriage
                 WHEN 1 THEN 'Married'
                 WHEN 2 THEN 'Single'
                 WHEN 3 THEN 'Others'
                 ELSE 'Unknown'
               END
        FROM silver.clients s;
        """,

        # 4. Populate dim_client
        """
        INSERT INTO gold.dim_client (client_id, limit_bal, sex_id, education_id, marriage_id, age)
        SELECT c.client_id, c.limit_bal, c.sex, c.education, c.marriage, c.age
        FROM silver.clients c;
        """,

        # 5. Populate dim_time
        """
        INSERT INTO gold.dim_time (time_id, year, month, month_name)
        SELECT 
            ROW_NUMBER() OVER (ORDER BY year, month) AS time_id,
            year,
            month,
            month_name
        FROM (
            SELECT DISTINCT
                EXTRACT(YEAR FROM ph.month)::int AS year,
                EXTRACT(MONTH FROM ph.month)::int AS month,
                TO_CHAR(ph.month, 'Month') AS month_name
            FROM silver.payment_history ph
        ) t;
        """,

        # 6. Populate fact_credit
        """
        INSERT INTO gold.fact_credit (
            fact_id,
            client_id,
            time_id,
            pay_status,
            bill_amount,
            payment_amount,
            default_flag
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY c.client_id, ph.month) AS fact_id,
            c.client_id,
            dt.time_id,
            ph.pay_status,
            bs.bill_amount,
            p.payment_amount,
            FALSE AS default_flag
        FROM silver.payment_history ph
        JOIN silver.bill_statements bs 
             ON ph.client_id = bs.client_id AND ph.month = bs.month
        JOIN silver.payments p 
             ON ph.client_id = p.client_id AND ph.month = p.month
        JOIN silver.clients c 
             ON ph.client_id = c.client_id
        JOIN gold.dim_time dt 
             ON dt.year = EXTRACT(YEAR FROM ph.month)::int
            AND dt.month = EXTRACT(MONTH FROM ph.month)::int;
        """
    ]

    try:
        with conn.cursor() as cur:
            for i, q in enumerate(queries, start=1):
                print(f"Running query {i} ...")
                cur.execute(q)
            conn.commit()
            print("Gold tables populated successfully ✅")
    except Exception as e:
        conn.rollback()
        print("Error during execution ❌:", e)
    finally:
        conn.close()
