FROM apache/airflow:2.6.1
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt