from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def hello_task():
    print("=" * 50)
    print("Hello from Airflow DAG!")
    print("This is a simple test that should work")
    print("=" * 50)
    return "SUCCESS"

def world_task():
    print("=" * 50)
    print("World! DAG is working correctly!")
    print("Task completed successfully")
    print("=" * 50)
    return "COMPLETED"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG that always works',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'simple'],
) as dag:

    task1 = PythonOperator(
        task_id='hello',
        python_callable=hello_task,
    )

    task2 = PythonOperator(
        task_id='world',
        python_callable=world_task,
    )

    task1 >> task2
