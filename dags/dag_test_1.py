import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Using BashOperator
with DAG(
dag_id='TEST-1',
schedule_interval=None,
start_date=datetime.datetime(2022, 4, 1),
catchup=False,
) as dag:
    sleep = BashOperator(
    task_id='sleep',
    bash_command='sleep 3',
    )