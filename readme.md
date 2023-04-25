# Local airflow development
Dev environment for airflow in docker

## How-to
Clone this repository and run the following commands:
docker-compose up airflow-init
docker-compose up

The airflow webserver will be available at: http://localhost:8080/
with the following credentials:
- user: airflow
- pass: airflow

## DAG
Airflow is set-up to sync with the local dags folder. The file dag_TP.py contains the tasks and operators that will generate the output files in the /dags/files/ directory