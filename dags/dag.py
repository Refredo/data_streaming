from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from scripts.script import start_streaming


with DAG(
    dag_id="random_names",
    start_date=datetime.datetime(2023, 10, 29),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    streaming_to_kafka = PythonOperator(
        task_id = 'streaming_to_kafka',
        python_callable=start_streaming
    )


    streaming_to_kafka