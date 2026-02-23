from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from random import choice

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def decide_branch():
    """Randomly choose a branch to execute"""
    branches = ['branch_a', 'branch_b', 'branch_c']
    selected = choice(branches)
    print(f"Selected branch: {selected}")
    return selected

def branch_task(branch_name):
    """Execute branch-specific logic"""
    print(f"Executing {branch_name}")
    return f"{branch_name} completed"

with DAG(
    'test_branching_workflow',
    default_args=default_args,
    description='Test DAG with conditional branching',
    schedule_interval='@hourly',
    catchup=False,
    tags=['test', 'branching'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting branching workflow"',
    )

    branch_decision = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
    )

    branch_a = PythonOperator(
        task_id='branch_a',
        python_callable=branch_task,
        op_kwargs={'branch_name': 'Branch A'},
    )

    branch_b = PythonOperator(
        task_id='branch_b',
        python_callable=branch_task,
        op_kwargs={'branch_name': 'Branch B'},
    )

    branch_c = PythonOperator(
        task_id='branch_c',
        python_callable=branch_task,
        op_kwargs={'branch_name': 'Branch C'},
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Workflow completed"',
        trigger_rule='none_failed_min_one_success',
    )

    start >> branch_decision >> [branch_a, branch_b, branch_c] >> end
