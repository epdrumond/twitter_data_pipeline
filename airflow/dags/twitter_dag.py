from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime
from os.path import join

datalake_path = '/home/edilson/Documents/data_pipeline/datalake'
with DAG(dag_id = 'twitter_dag', start_date = datetime.now()) as dag:
    twitter_operator = TwitterOperator(
    task_id = 'retrieve_twitter_data',
    query = '@FortalezaEC',
    file_path = join(
        datalake_path,
        'fortaleza_ec',
        'extract_date={{ ds }}',
        'twitter_dump_{{ ds_nodash }}.json'
    ))

    twitter_transform = SparkSubmitOperator(
        task_id = 'transform_tweet_data',
        application = '/home/edilson/Documents/data_pipeline/spark/transformation.py',
        name = 'twitter_transformation',
        application_args = [
            '--src',
            '/home/edilson/Documents/data_pipeline/datalake/bronze/fortaleza_ec/extract_date=2022-02-26',
            '--dest',
            '/home/edilson/Documents/data_pipeline/datalake/silver/fortaleza_ec',
            '--process-date',
            '{{ ds }}'
        ]
    )
