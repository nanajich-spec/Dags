from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello World from Test DAG!")
    return "Success"

def print_date():
    print(f"Current date: {datetime.now()}")
    return "Date printed"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_hello_world',
    default_args=default_args,
    description='Test Hello World DAG',
    schedule='@daily',
    catchup=False,
    tags=['test', 'hello-world'],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    date_task = PythonOperator(
        task_id='print_current_date',
        python_callable=print_date,
    )

    hello_task >> date_task
