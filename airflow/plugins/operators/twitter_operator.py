from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

from pathlib import Path
from os.path import join
from datetime import datetime, timedelta
import json

class TwitterOperator(BaseOperator):

    template_fields = ['query', 'file_path', 'start_time', 'end_time']

    @apply_defaults
    def __init__(
        self,
        query, file_path, conn_id = None, start_time = None, end_time = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents = True, exist_ok = True)

    def execute(self, context):
        hook = TwitterHook(
            query = self.query,
            conn_id = self.conn_id,
            start_time = self.start_time,
            end_time = self.end_time
        )
        self.create_parent_folder()
        with open(self.file_path, 'w') as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii = False)
                output_file.write('\n')


if __name__ == '__main__':
    start_date = datetime.today().date() - timedelta(days = 1)
    end_date = start_date + timedelta(days = 1)

    date_format = '%Y-%m-%dT%H:%M:%S.00Z'
    start_date = start_date.strftime(date_format)
    end_date = end_date.strftime(date_format)

    datalake_path = '/home/edilson/Documents/data_pipeline/datalake'
    with DAG(dag_id = 'twitter_test', start_date = datetime.now()) as dag:
        test_operator = TwitterOperator(
            query = '@FortalezaEC',
            file_path = join(
                datalake_path,
                'fortaleza_ec',
                'extract_date={{ ds }}',
                'twitter_dump_{{ ds_nodash }}.json'
            ),
            task_id = 'test_twitter_operator',
            start_time = start_date,
            end_time = end_date
        )
        test_task = TaskInstance(
            task = test_operator,
            execution_date = datetime.now()
        )
        test_task.run()
