from datetime import datetime, timedelta

import pendulum

from airflow import DAG

# from airflow.models import Variable

# from airflow.utils.task_group import TaskGroup

# from airflow.sensors.external_task import ExternalTaskSensor

# from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

# from airflow.hooks.base import BaseHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


# Конфигурация DAG
OWNER = 'korsak0v'
DAG_ID = 'simple_dag'
LOCAL_TZ = pendulum.timezone('Europe/Moscow')

# Названия коннекторов к GP
PG_CONNECT = 'test_db'

# Используемые таблицы в DAG
PG_TARGET_SCHEMA = 'dm'
PG_TARGET_TABLE = 'fct_'
INDEX_KPI = 1

sql_query = 'SELECT 1 AS one'

LONG_DESCRIPTION = '# LONG_DESCRIPTION'

SHORT_DESCRIPTION = 'SHORT_DESCRIPTION'


args = {
    'owner': OWNER,
    'start_date': datetime(2023, 1, 1, tzinfo=LOCAL_TZ),
    'catchup': True,
    'retries': 3,
    'retry_delay': timedelta(hours=1),
}

with DAG(
        dag_id=DAG_ID,
        schedule_interval='10 0 * * *',
        default_args=args,
        tags=['check_pg_connect', 'test'],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id='start',
    )

    drop_tmp_before = PostgresOperator(
        task_id='drop_tmp_before',
        sql=f'''DROP TABLE IF EXISTS stg.tmp_{PG_TARGET_TABLE}''',
        postgres_conn_id=PG_CONNECT
    )

    create_tmp = PostgresOperator(
        task_id='create_tmp',
        sql=f'''
        CREATE TABLE stg.tmp_{PG_TARGET_TABLE} AS
        {sql_query};
        ''',
        postgres_conn_id=PG_CONNECT
    )

    delete_from_target = PostgresOperator(
        task_id='delete_from_target',
        sql=f'''
        DELETE FROM {PG_TARGET_SCHEMA}.{PG_TARGET_TABLE}
        WHERE 
            date IN (
                SELECT 
                    date 
                FROM 
                    stg.tmp_{PG_TARGET_TABLE}
                WHERE
                    "index" = {INDEX_KPI}
                )
        AND "index" = {INDEX_KPI}
        ''',
        postgres_conn_id=PG_CONNECT
    )

    insert_from_tmp_to_target = PostgresOperator(
        task_id='insert_from_tmp_to_target',
        sql=f'''
        INSERT INTO TARGET TABLE {PG_TARGET_SCHEMA}.{PG_TARGET_TABLE}("date", value, "index")
        SELECT 
            "date", 
            value, 
            "index" 
        FROM 
            stg.tmp_{PG_TARGET_TABLE}
        ''',
        postgres_conn_id=PG_CONNECT
    )

    drop_tmp_after = PostgresOperator(
        task_id='drop_tmp_after',
        sql=f'''DROP TABLE IF EXISTS stg.tmp_{PG_TARGET_TABLE}''',
        postgres_conn_id=PG_CONNECT
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> drop_tmp_before >> create_tmp >> delete_from_target >> insert_from_tmp_to_target >> drop_tmp_after >> end
    