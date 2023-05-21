import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Using a DAG decorator to turn a function into a DAG generator
@dag(
    dag_id="AWS-TEST",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    #dagrun_timeout=datetime.timedelta(minutes=60),
)

def ProcessCSV():
    @task
    def FiltrarDatos():
        import pandas as pd

        s3_input = "magus-udesa-pa-raw"
        s3_output = "magus-udesa-pa-intermediate"

        df_ids = pd.read_csv(f"s3://{s3_input}/advertiser_ids", header=0) # Load all advertisers
        df_ads = pd.read_csv(f"s3://{s3_input}/ads_views", header=0) # Load all ads views

    
    [FiltrarDatos()]


dag = ProcessCSV()