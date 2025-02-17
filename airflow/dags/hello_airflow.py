from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define a simple Python function
def hello_airflow():
    print("Hello, Airflow!")

# Define default arguments for the DAG
default_args = {
    "owner": "abhinav",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    schedule=None,
    description="A simple Hello World DAG",
    schedule_interval=None,  # Runs daily at 9 AM
    catchup=False,
    tags=["abhinav"],
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_airflow,
    )

    # Set task dependencies (only one task in this case)
    hello_task

