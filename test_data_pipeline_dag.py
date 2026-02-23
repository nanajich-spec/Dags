from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def extract_data():
    """Simulate data extraction"""
    print("Extracting data from source...")
    return {'records': 1000, 'status': 'extracted'}

def transform_data():
    """Simulate data transformation"""
    print("Transforming data...")
    return {'records': 950, 'status': 'transformed', 'dropped': 50}

def validate_data():
    """Simulate data validation"""
    print("Validating data quality...")
    return {'valid': True, 'errors': 0}

def load_data():
    """Simulate data loading"""
    print("Loading data to destination...")
    return {'loaded': 950, 'status': 'completed'}

with DAG(
    'test_data_pipeline',
    default_args=default_args,
    description='Test ETL data pipeline DAG',
    schedule='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['test', 'etl', 'data-pipeline'],
) as dag:

    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting ETL pipeline at $(date)"',
    )

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up temporary files..."',
    )

    notify = BashOperator(
        task_id='notify_completion',
        bash_command='echo "Pipeline completed successfully at $(date)"',
    )

    # ETL workflow
    start >> extract >> transform >> validate >> load >> cleanup >> notify
