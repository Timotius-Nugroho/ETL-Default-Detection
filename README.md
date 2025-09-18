# Data Engineering Toolkit

This project is a **data engineering toolkit** that uses **Apache Airflow** to orchestrate end-to-end data processing tasks, from raw ingestion to prediction.

---

## 1. Project Background

Credit default (payment failure) is one of the major risks in the credit card and loan business.  
If it cannot be predicted early, banks and financial institutions may suffer significant losses, which in turn affects the overall quality of their credit portfolio.  

**Project Objectives:**
- Build a data pipeline system capable of processing customer transaction data into a prediction-ready dataset.  
- Develop a machine learning model to predict the likelihood of default in the next month’s payment.  
- Provide business insights on how demographic factors and credit limits influence default risk, enabling the risk management team to design more effective policies.  

---

## 2. Problem Statement

- How can we detect potential credit default early based on historical billing, payment, and customer profile data?  
- Are there observable patterns within demographic groups (e.g., age) and credit limits that significantly affect default probability?  
- How can the data pipeline be designed to be automated, clean, and ready for both analytics and machine learning use cases?  

---

## 3. Data Platform Design

The architecture follows a layered data pipeline:  

![Data Platform Design](https://github.com/user-attachments/assets/323ab4b5-480c-4e57-a88d-4f46b5e8656c)

- **Source (External Schema)**  
  Credit application database: `clients`, `bill_statements`, `payment_history`, `payments`.

- **Silver Layer**  
  Cleansed/staged data (duplicate removal, type casting, standardized dates).

- **Gold Layer (Data Warehouse – Snowflake schema)**  
  - Dimensions: `dim_client`, `dim_sex`, `dim_education`, `dim_marriage`, `dim_time`.  
  - Fact: `fact_credit`.

- **Prediction Layer (`tmp_ml_pred`)**  
  Flattened dataset prepared for machine learning.

- **Prediction Service**  
  - ML job (`run_prediction`) orchestrated by Airflow.  
  - Output: `flag default_payment_next_month`.

**Tools Used**:
- **Airflow** → ETL orchestration.  
- **PostgreSQL / DWH** → Data storage (Silver, Gold).  
- **ML Model (scikit-learn / Vertex AI)** → Default payment prediction.  

---

## 4. Folder Structure

data-eng-toolkit/
└── airflow/
 └── dags/
  ├── credit_data_processing.py
  └── resources/
  ├── scripts/  # Python scripts for each task
  └── utils/    # Utilities (e.g., db_conn.py)

- `data-eng-toolkit/airflow/dags/` → Contains the Airflow DAG definition.  
- `data-eng-toolkit/airflow/dags/resources/scripts/` → Python scripts for DAG tasks.  
- `data-eng-toolkit/airflow/dags/resources/utils/` → Utility scripts (e.g., database connection).  

---

## 5. DAG: `credit_data_processing`

The DAG is defined in  
`data-eng-toolkit/airflow/dags/credit_data_processing.py`.

### Tasks
1. **cleanup_task** → Data cleanup.  
   Script: `resources/scripts/cleanup_data.py`  
2. **populate_silver_task** → Load cleansed data into Silver.  
   Script: `resources/scripts/populate_silver.py`  
3. **populate_gold_task** → Load transformed data into Gold.  
   Script: `resources/scripts/populate_gold.py`  
4. **prepare_prediction_dataset_task** → Build flattened dataset for ML.  
   Script: `resources/scripts/prepare_prediction_dataset.py`  
5. **run_prediction_task** → Run ML model for default prediction.  
   Script: `resources/scripts/run_prediction.py`  

---

## 6. Data Modeling

![Data Modeling](https://github.com/user-attachments/assets/93d7b75c-df10-456e-814d-2b4e025f3512)

- **Silver Layer**  
  Mirrors the source schema, but with cleansed and standardized data.  

- **Gold Layer (Snowflake Schema)**  
  - Dimensions: demographic, time, limit, etc.  
  - Fact: transaction and payment history (`fact_credit`).  
  - Rationale: Enables OLAP-style queries and business insights.  

- **Prediction Layer (`tmp_ml_pred`)**  
  Flattened tabular dataset prepared for ML algorithms.  
  - Example fields: `pay_0 … pay_6`, `bill_amt1 … bill_amt6`, `pay_amt1 … pay_amt6`.  
  - Label: `default_payment_next_month`.  

---

## 7. Database Connection

The database connection utility is located at:  
`data-eng-toolkit/airflow/dags/resources/utils/db_conn.py`.

---

## 8. How to Run

1. Ensure **Airflow** is running.  
2. Enable the `credit_data_processing` DAG in the **Airflow UI**.  
3. Trigger the DAG manually or wait for the scheduled run.  
