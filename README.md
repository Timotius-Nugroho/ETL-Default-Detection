# Data Engineering Toolkit

This project is a data engineering toolkit that uses Airflow to orchestrate data processing tasks.

## Folder Structure

*   `data-eng-toolkit/airflow/dags/`: Contains the Airflow DAG definition.
*   `data-eng-toolkit/airflow/dags/resources/scripts/`: Contains the Python scripts for each task in the DAG.
*   `data-eng-toolkit/airflow/dags/resources/utils/`: Contains utility scripts, such as database connections.

## DAG: `credit_data_processing`

The `credit_data_processing` DAG is defined in `data-eng-toolkit/airflow/dags/credit_data_processing.py`. This DAG orchestrates the following tasks:

1.  **cleanup_task:** Cleans up the data. The script is located at `data-eng-toolkit/airflow/dags/resources/scripts/cleanup_data.py`.
2.  **populate_silver_task:** Populates the silver table. The script is located at `data-eng-toolkit/airflow/dags/resources/scripts/populate_silver.py`.
3.  **populate_gold_task:** Populates the gold table. The script is located at `data-eng-toolkit/airflow/dags/resources/scripts/populate_gold.py`.
4.  **prepare_prediction_dataset_task:** Prepares the dataset for prediction. The script is located at `data-eng-toolkit/airflow/dags/resources/scripts/prepare_prediction_dataset.py`.
5.  **run_prediction_task:** Runs the prediction model. The script is located at `data-eng-toolkit/airflow/dags/resources/scripts/run_prediction.py`.

## Database Connection

The database connection is managed in `data-eng-toolkit/airflow/dags/resources/utils/db_conn.py`.

## How to Run

1.  Make sure you have Airflow running.
2.  Enable the `credit_data_processing` DAG in the Airflow UI.
3.  Trigger the DAG manually or wait for the scheduled run.
