from builtins import range
from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import requests

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(
    dag_id='start_word_count',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=1),
)

def submit_word_count():
    url = "http://spark-master:8998/batches"

    payload = "{\n  \"file\": \"/usr/local/data/jar/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar\",\n  \"className\": \"thoughtworks.wordcount.WordCount\",\n  \"args\": [\"/usr/local/data/input/\", \"/usr/local/data/output\"]\n}"
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "47a9b016-b3bb-0d3b-74ae-285a1e807a06"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    print("Submit response:", response.text)


cleanup_output_dir = BashOperator(
    task_id='cleanup_output_dir',
    bash_command='rm -rf /usr/local/data/output',
    dag=dag,
)

trigger_word_count = PythonOperator(
    task_id='trigger_word_count',
    python_callable=submit_word_count,
    dag=dag,
)

cleanup_output_dir >> trigger_word_count

if __name__ == "__main__":
    dag.cli()

