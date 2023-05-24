import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Using a DAG decorator to turn a function into a DAG generator
@dag(
    dag_id="TP-pipeline-AWS",
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
            df.drop(df.columns[0], axis=1)
            .groupby(['advertiser_id','product_id', 'date'])
            .value_counts()
            .groupby(level=0, group_keys=False)
            .nlargest(20)
            .reset_index(name='count')
        )
        print(df.head())
        print("---------------------------------")
        print(df_out.head(5))
        df_out.to_csv(f"s3://{s3_bucket}/ctr", sep=',', header=True)
        df_out.to_csv(f"ctr.csv", sep=',', header=True)

    @task
    def TopProduct():
        # Compute top 20 (or less) products seen in advertisers website, for each advertiser 
        import pandas as pd

        s3_bucket = "magus-udesa-pa-intermediate"

        df = pd.read_csv(f"s3://{s3_bucket}/valid_products",header=0)

        df_out = (
            df.drop(df.columns[0], axis=1)
            .groupby(['advertiser_id','product_id', 'date'])
            .value_counts()
            .groupby(level=0, group_keys=False)
            .nlargest(20)
            .reset_index(name='count')
        )
        #df_out.to_csv(f"s3://{s3_bucket}/topproduct", sep=',', header=True)
        df_out.to_csv(f"topproduct.csv", sep=',', header=True)
    
    @task
    def DBWriting():
        import pandas as pd

        # Read CSV file
        s3_bucket = "magus-udesa-pa-intermediate"
        csv_file = f"s3://{s3_bucket}/ctr"
        df = pd.read_csv(csv_file)
        
        # Establish connection to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_TP')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        # Insert data into the table
        for index, row in df.iterrows():
            insert_query = f" \
            INSERT INTO topctr (advertiser_id, product_id, date, ctr) \
            VALUES (\'{row['advertiser_id']}\', \'{row['product_id']}\', \'{row['date']}\', {row['count']})"
            cursor.execute(insert_query)
            connection.commit()
        
        # Close the database connection
        cursor.close()
        connection.close()

        # Read CSV file
        s3_bucket = "magus-udesa-pa-intermediate"
        csv_file = f"s3://{s3_bucket}/topproduct"
        df = pd.read_csv(csv_file)
        
        # Establish connection to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_TP')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        # Insert data into the table
        for index, row in df.iterrows():
            insert_query = f" \
            INSERT INTO topproduct (advertiser_id, product_id, date, top_product) \
            VALUES (\'{row['advertiser_id']}\', \'{row['product_id']}\', \'{row['date']}\', {row['count']})"
            cursor.execute(insert_query)
            connection.commit()
        
        # Close the database connection
        cursor.close()
        connection.close()

    [FiltrarDatos() >> [TopCTR(), TopProduct()] >> DBWriting()]


dag = ProcessCSV()