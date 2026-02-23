from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def process_data(task_id):
    """Simulate data processing"""
    print(f"Processing data in {task_id}")
    return f"Data processed by {task_id}"

with DAG(
    'test_parallel_tasks',
    default_args=default_args,
    description='Test DAG with parallel task execution',
    schedule='@daily',
    catchup=False,
    tags=['test', 'parallel'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting parallel processing"',
    )

    # Create parallel tasks
    parallel_tasks = []
    for i in range(5):
        task = PythonOperator(
            task_id=f'process_task_{i}',
            python_callable=process_data,
            op_kwargs={'task_id': f'task_{i}'},
        )
        parallel_tasks.append(task)

    end = BashOperator(
        task_id='end',
        bash_command='echo "All parallel tasks completed"',
    )

    # Set dependencies: start -> all parallel tasks -> end
    start >> parallel_tasks >> end
