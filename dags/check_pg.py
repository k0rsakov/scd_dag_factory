from datetime import datetime, timedelta

import pendulum

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Конфигурация DAG
OWNER = 'i.korsakov'
DAG_ID = 'test_templates'
LOCAL_TZ = pendulum.timezone('Europe/Moscow')

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    'owner': OWNER,
    'start_date': datetime(2023, 1, 20, tzinfo=LOCAL_TZ),
    'catchup': True,
    'retries': 3,
    'retry_delay': timedelta(hours=1),
}


def check_pg_connect(**context):
    """"""
    pg = PostgresHook('test_db')

    df = pg.get_pandas_df('select * from test')

    print(df)

with DAG(
        dag_id=DAG_ID,
        schedule_interval='10 0 * * *',
        default_args=args,
        tags=['templates', 'test'],
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    check_pg_connect = PythonOperator(
        task_id='check_pg_connect',
        python_callable=check_pg_connect,
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> check_pg_connect >> end
