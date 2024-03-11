from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_project',
    default_args=default_args,
    description='Orchestrate Extraction, Transformation, and Load processes',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
)

def run_extraction():
    from Extraction import postgres_conn, Extract
    pg_conn = postgres_conn()
    extract_tables = ['infant_death_rate', 'Countries', 'Paises', 'Population', 'life_expectancy']
    for table_name in extract_tables:
        Extract(pg_conn, table_name)

def run_transformation():
    from Transformation import transform
    transform()

def run_load():
    from Load import Load
    Load()

with dag:
    task_extraction = PythonOperator(
        task_id='run_extraction',
        python_callable=run_extraction,
    )

    task_transformation = PythonOperator(
        task_id='run_transformation',
        python_callable=run_transformation,
    )

    task_load = PythonOperator(
        task_id='run_load',
        python_callable=run_load,
    )

task_extraction >> task_transformation >> task_load
