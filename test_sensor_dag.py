from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from random import random

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_condition():
    """Simulate checking if a condition is met"""
    result = random() > 0.3
    print(f"Condition check: {result}")
    return result

def process_after_condition():
    """Process data after condition is met"""
    print("Condition met! Processing data...")
    return "Processing completed"

with DAG(
    'test_sensor_workflow',
    default_args=default_args,
    description='Test DAG with sensor waiting for conditions',
    schedule='@daily',
    catchup=False,
    tags=['test', 'sensor'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting sensor workflow"',
    )

    wait_for_condition = PythonSensor(
        task_id='wait_for_condition',
        python_callable=check_condition,
        poke_interval=30,  # Check every 30 seconds
        timeout=300,  # Timeout after 5 minutes
        mode='poke',
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_after_condition,
    )

    notify = BashOperator(
        task_id='notify_success',
        bash_command='echo "Sensor workflow completed successfully"',
    )

    start >> wait_for_condition >> process_data >> notify
