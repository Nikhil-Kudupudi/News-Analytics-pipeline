from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag=DAG(
    dag_id='news-fetch-dag',
    default_args={},
    start_date=datetime.now(),
    schedule='@daily'
)