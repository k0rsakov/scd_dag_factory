from datetime import datetime, timedelta

import pendulum

from airflow import DAG

# from airflow.sensors.external_task import ExternalTaskSensor

from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Конфигурация DAG
OWNER = 'korsak0v'
DAG_ID = 'simple_dag'
LOCAL_TZ = pendulum.timezone('Europe/Moscow')

# Названия коннекторов к GP
PG_CONNECT = 'test_db'

# Используемые таблицы в DAG
PG_TARGET_SCHEMA = 'dm'
PG_TARGET_TABLE = 'fct_sales'
PG_TMP_SCHEMA = 'stg'
PG_TMP_TABLE = f'tmp_{PG_TARGET_TABLE}_{{{{ data_interval_start.format("YYYY_MM_DD") }}}}'
INDEX_KPI = 1

sql_query = '''
SELECT 
	('2023-'||((random()*11+1)::int)::varchar||'-'||((random()*27+1)::int)::varchar)::date AS date,
	(random()*100)::int AS value,
	1 AS kpi_id
'''

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
        tags=['dm'],
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
        sql=f'''DROP TABLE IF EXISTS {PG_TMP_SCHEMA}.{PG_TMP_TABLE}''',
        postgres_conn_id=PG_CONNECT
    )

    create_tmp = PostgresOperator(
        task_id='create_tmp',
        sql=f'''
        CREATE TABLE {PG_TMP_SCHEMA}.{PG_TMP_TABLE} AS
        {
            sql_query.format(
                start_date="{{ data_interval_start.format('YYYY-MM-DD') }}",
                end_date="{{ data_interval_end.format('YYYY-MM-DD') }}"
            )
        };
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
                    {PG_TMP_SCHEMA}.{PG_TMP_TABLE}
                )
        ''',
        postgres_conn_id=PG_CONNECT
    )

    insert_from_tmp_to_target = PostgresOperator(
        task_id='insert_from_tmp_to_target',
        sql=f'''
        INSERT INTO {PG_TARGET_SCHEMA}.{PG_TARGET_TABLE}("date", value, kpi_id)
        SELECT 
            "date", 
            value,
            kpi_id
        FROM 
            {PG_TMP_SCHEMA}.{PG_TMP_TABLE}
        ''',
        postgres_conn_id=PG_CONNECT
    )

    drop_tmp_after = PostgresOperator(
        task_id='drop_tmp_after',
        sql=f'''DROP TABLE IF EXISTS {PG_TMP_SCHEMA}.{PG_TMP_TABLE}''',
        postgres_conn_id=PG_CONNECT
    )

    end = EmptyOperator(
        task_id='end',
    )

    start >> drop_tmp_before >> create_tmp >> delete_from_target >> insert_from_tmp_to_target >> drop_tmp_after >> end
    