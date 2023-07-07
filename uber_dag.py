from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from uber_etl import uber_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    'uber_dag',
    default_args=default_args,
    description='Uber - ETL dag',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='Uber_ETL',
    python_callable=uber_etl,
    dag=dag, 
)

run_etl