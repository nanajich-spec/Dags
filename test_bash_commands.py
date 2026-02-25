from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'test_bash_commands',
    default_args=default_args,
    description='Test DAG with Bash commands',
    schedule='@hourly',
    catchup=False,
    tags=['test', 'bash'],
) as dag:

    t1 = BashOperator(
        task_id='list_files',
        bash_command='ls -la /opt/airflow',
    )

    t2 = BashOperator(
        task_id='check_hostname',
        bash_command='hostname',
    )

    t3 = BashOperator(
        task_id='check_disk_space',
        bash_command='df -h',
    )

    t4 = BashOperator(
        task_id='echo_success',
        bash_command='echo "All tests passed!"',
    )

    [t1, t2, t3] >> t4
