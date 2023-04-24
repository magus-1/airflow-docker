from airflow.operators.python import PythonOperator
import numpy as np

def sample_normal(mean, std):
    return np.random.normal(loc=mean, scale=std)

# Using PythonOperator
random_height = PythonOperator(
    task_id='height',
    python_callable=sample_normal,
    op_kwargs = {"mean" : 170, "std": 15},
    )