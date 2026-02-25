from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def generate_data(**context):
    """Generate data and push to XCom"""
    data = {
        'user_count': 1500,
        'transaction_amount': 45000.50,
        'processing_time': '2.5s'
    }
    print(f"Generated data: {data}")
    return data

def process_data(**context):
    """Pull data from XCom and process it"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='generate_data')
    print(f"Received data: {data}")
    
    processed = {
        'users': data['user_count'] * 2,
        'revenue': data['transaction_amount'] * 1.1,
        'status': 'processed'
    }
    print(f"Processed data: {processed}")
    return processed

def aggregate_results(**context):
    """Pull and aggregate all previous results"""
    ti = context['ti']
    generated = ti.xcom_pull(task_ids='generate_data')
    processed = ti.xcom_pull(task_ids='process_data')
    
    summary = {
        'original_users': generated['user_count'],
        'processed_users': processed['users'],
        'revenue': processed['revenue'],
        'pipeline_status': 'completed'
    }
    print(f"Final summary: {summary}")
    return summary

with DAG(
    'test_xcom_communication',
    default_args=default_args,
    description='Test DAG with XCom for task communication',
    schedule='@daily',
    catchup=False,
    tags=['test', 'xcom'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting XCom workflow"',
    )

    generate = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
    )

    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    aggregate = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "XCom workflow completed"',
    )

    start >> generate >> process >> aggregate >> end
