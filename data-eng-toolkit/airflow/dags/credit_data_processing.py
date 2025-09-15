from airflow.decorators import dag, task
from pendulum import datetime
from resources.scripts.cleanup_data import main as cleanup_data
from resources.scripts.populate_silver import main as populate_silver
from resources.scripts.populate_gold import main as populate_gold
from resources.scripts.prepare_prediction_dataset import main as prepare_prediction_dataset
from resources.scripts.run_prediction import main as run_prediction
from resources.scripts.run_math_prediction import main as run_math_prediction

@dag(
    schedule="0 0 1 * *", 
    start_date=datetime(2024, 1, 1), 
    catchup=False,
    tags=["credit", "etl"],
)
def credit_data_processing():

    @task(task_id="cleanup_task")
    def cleanup():
        cleanup_data()

    @task(task_id="populate_silver_task")
    def silver():
        populate_silver()

    @task(task_id="populate_gold_task")
    def gold():
        populate_gold()

    @task(task_id="prepare_prediction_dataset_task")
    def prepare():
        prepare_prediction_dataset()

    @task(task_id="run_prediction_task")
    def predict():
        run_math_prediction()

    cleanup() >> silver() >> gold() >> prepare() >> predict()


credit_data_processing()