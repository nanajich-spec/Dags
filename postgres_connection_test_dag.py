"""
DAG to test Postgres connection in Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def test_postgres_connection(**context):
    """
    Test Postgres connection using PostgresHook
    """
    try:
        # Replace 'postgres_default' with your connection ID from Airflow UI
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Test connection
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Execute a simple query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print(f"✅ Connection successful!")
        print(f"PostgreSQL version: {version[0]}")
        
        # Get some basic stats
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()
        print(f"Connected to database: {db_name[0]}")
        
        cursor.close()
        conn.close()
        
        return "Connection test passed successfully"
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        raise

with DAG(
    'postgres_connection_test',
    default_args=default_args,
    description='Test Postgres connection configured in Airflow UI',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'postgres', 'connection'],
) as dag:
    
    # Task 1: Test connection using Python function
    test_connection_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
        provide_context=True,
    )
    
    # Task 2: Execute a simple SQL query using PostgresOperator
    execute_query_task = PostgresOperator(
        task_id='execute_simple_query',
        postgres_conn_id='postgres_default',  # Change this to your connection ID
        sql="""
            SELECT 
                'Connection test successful' as status,
                current_timestamp as test_time,
                current_database() as database_name,
                current_user as connected_user;
        """,
    )
    
    # Task 3: List all tables in public schema
    list_tables_task = PostgresOperator(
        task_id='list_tables',
        postgres_conn_id='postgres_default',  # Change this to your connection ID
        sql="""
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """,
    )
    
    # Set task dependencies
    test_connection_task >> execute_query_task >> list_tables_task
