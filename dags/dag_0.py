# Testing a defult file
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,4,20),
    'retries': 0,
}

dag = DAG(
    dag_id='DAG-0',
    default_args=default_args,
    catchup=False,
    schedule_interval='@once',
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)