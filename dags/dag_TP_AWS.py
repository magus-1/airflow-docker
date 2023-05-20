import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Using a DAG decorator to turn a function into a DAG generator
@dag(
    dag_id="TP-pipeline-local",
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

        # Filter valid advertisers IDs
        df_output = (
            df_ads.merge(df_ids, 
                    on=['advertiser_id'],
                    how='left', 
                    indicator=True)
            .query('_merge == "both"')
            .drop(columns='_merge')
        )
        df_output.to_csv(f"s3://{s3_output}/valid_ads", sep=',', header=True)

        # Filter product views of valid advertiser IDs
        df_products = pd.read_csv(f"s3://{s3_input}/product_views", header=0)

        df_output = (
            df_products.merge(df_ids, 
                    on=['advertiser_id'],
                    how='left', 
                    indicator=True)
            .query('_merge == "both"')
            .drop(columns='_merge')
        )
        df_output.to_csv(f"s3://{s3_output}/valid_products", sep=',', header=True)

    @task
    def TopCTR():
        # Compute top 20 (or less) products that generated clicks, for each advertiser 
        import pandas as pd

        s3_bucket = "magus-udesa-pa-intermediate"

        df = pd.read_csv(f"s3://{s3_bucket}/valid_ads",header=0)
        df = df[df['type']=='click']

        df_out = (
            df.groupby(['advertiser_id']).product_id.value_counts()
            .groupby(level=0, group_keys=False)
            .nlargest(20)
        )
        df_out.to_csv(f"s3://{s3_bucket}/ctr", sep=',', header=True)

    @task
    def TopProduct():
        # Compute top 20 (or less) products seen in advertisers website, for each advertiser 
        import pandas as pd

        s3_bucket = "magus-udesa-pa-intermediate"

        df = pd.read_csv(f"s3://{s3_bucket}/valid_products",header=0)

        df_out = (
            df.groupby(['advertiser_id']).product_id.value_counts()
            .groupby(level=0, group_keys=False)
            .nlargest(20)
        )
        df_out.to_csv(f"s3://{s3_bucket}/topproduct", sep=',', header=True)
    
    '''
    # TODO: add support for RDS
    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )

    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )
    '''

    [FiltrarDatos() >> [TopCTR(), TopProduct()]]


dag = ProcessCSV()