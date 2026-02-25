from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'example_bash_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Print the date
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Task 2: Sleep for 5 seconds
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    # Task 3: Print hello world
    t3 = BashOperator(
        task_id='print_hello',
        depends_on_past=False,
        bash_command='echo "Hello World from Airflow!"',
    )

    # Define task dependencies
    t1 >> t2 >> t3
