from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def calculate_sum(a, b):
    result = a + b
    print(f"Sum of {a} + {b} = {result}")
    return result

def multiply_numbers(a, b):
    result = a * b
    print(f"Product of {a} * {b} = {result}")
    return result

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'test_math_operations',
    default_args=default_args,
    description='Simple math operations test',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['test', 'math', 'python'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting math operations..."',
    )

    add_task = PythonOperator(
        task_id='add_numbers',
        python_callable=calculate_sum,
        op_kwargs={'a': 10, 'b': 20},
    )

    multiply_task = PythonOperator(
        task_id='multiply_numbers',
        python_callable=multiply_numbers,
        op_kwargs={'a': 5, 'b': 7},
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Math operations completed!"',
    )

    start >> [add_task, multiply_task] >> end
