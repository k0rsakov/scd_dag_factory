from datetime import datetime, timedelta

import pendulum

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_kpi_of_dag(
        owner: str = None,
        dag_id: str = None,
        pg_target_schema: str = None,
        pg_target_table: str = None,
        index_kpi: str = None,
        pg_environment: str = None,
        airflow_environment: str = None,
        long_description: str = None,
        short_description: str = None,
        start_date: str = None,
        cron_expression: str = None,
        tags: str = None,
        sensors: str = None,
        sql_query: str = None,
) -> DAG:
    """
    Функция, которая генерирует типовой DAG для получения метрики.

    Вся логика описана и прокомментирована внутри функции.

    Некоторые моменты обработаны исключительно для функции, чтобы обработать какие-то атрибуты и получить желаемый
    эффект.

    @param owner: Владелец DAG.
    @param dag_id: Название DAG.
    @param pg_target_schema: Целевая схема в PostgreSQL.
    @param pg_target_table: Целевая таблица в PostgreSQL.
    @param index_kpi: ID показателя.
    @param pg_environment: Окружение PostgreSQL.
    @param airflow_environment: Окружение Airflow.
    @param long_description: Полное описание отчета.
    @param short_description: Короткое описание отчета.
    @param start_date: Дата начала работы DAG.
    @param cron_expression: Cron.
    @param tags: Tags.
    @param sensors: Sensors.
    @param sql_query: SQL-запрос для получения метрики.
    @return: Возвращает DAG.
    """
    # Конфигурация DAG
    local_tz = pendulum.timezone('Europe/Moscow')

    # Используемые таблицы в DAG
    pg_target_schema = pg_target_schema
    pg_target_table = pg_target_table
    pg_tmp_schema = 'stg'
    pg_tmp_table = f'tmp_{dag_id}_{{{{ data_interval_start.format("YYYY_MM_DD") }}}}'

    # Названия коннекторов к GP
    pg_connect = 'test_db' if pg_environment == 'dev' else 'test_db_prod'

    # Сделана заглушка атрибута. Это можно использовать для указания разных сценариев в зависимости от окружения
    airflow_environment = airflow_environment

    # Дата приходит в формате str и после парсинга, мы можем получить дату и любые элементы даты
    parse_date = pendulum.parse(start_date)

    args = {
        'owner': owner,
        'start_date': datetime(parse_date.year, parse_date.month, parse_date.day, tzinfo=local_tz),
        'catchup': True,
        'depends_on_past': True,
        'retries': 3,
        'retry_delay': timedelta(hours=1),
    }

    # Tags приходят в str формате, поэтому нужно их правильно "разобрать" и превратить в list
    raw_tags = list(tags.split(','))
    tags_ = []

    for i in raw_tags:
        tags_.append(  # noqa: PERF401
            i.replace("'", "")
            .replace(" ", '')
            .replace("[", "")
            .replace("]", "")
        )

    # Sensors приходят в str формате, поэтому нужно их правильно "разобрать" и превратить в list
    if sensors:
        raw_sensors = list(sensors.split(','))
        sensors_ = []
        for i in raw_sensors:
            sensors_.append(  # noqa: PERF401
                i.replace("'", "")
                .replace(' ', '')
                .replace("[", "")
                .replace("]", "")
            )
    else:
        sensors_ = None

    with DAG(
            dag_id=dag_id,
            schedule_interval=cron_expression,
            default_args=args,
            tags=tags_,
            description=short_description,
            concurrency=1,
            max_active_tasks=1,
            max_active_runs=1,
    ) as dag:
        dag.doc_md = long_description

        start = EmptyOperator(
            task_id='start',
        )

        # Если есть sensors, то мы создаем задачи с сенсорами, иначе создаем одну пустышку
        if sensors_:
            sensors_task = [
                ExternalTaskSensor(
                    task_id=f'sensor_{dag}',
                    external_dag_id=dag,
                    allowed_states=['success'],
                    mode='reschedule',
                    timeout=360000,  # длительность работы сенсора
                    poke_interval=600  # частота проверки
                ) for dag in sensors_
            ]
        else:
            sensors_task = [EmptyOperator(task_id=f'empty_{value}') for value in range(1)]

        drop_tmp_before = PostgresOperator(
            task_id='drop_tmp_before',
            sql=f'''DROP TABLE IF EXISTS {pg_tmp_schema}.{pg_tmp_table}''',
            postgres_conn_id=pg_connect
        )

        create_tmp = PostgresOperator(
            task_id='create_tmp',
            sql=f'''
            CREATE TABLE {pg_tmp_schema}.{pg_tmp_table} AS
            {
                sql_query.format(
                    start_date="{{ data_interval_start.format('YYYY-MM-DD') }}",
                    end_date="{{ data_interval_end.format('YYYY-MM-DD') }}"
                )
            };
            ''',
            postgres_conn_id=pg_connect
        )

        delete_from_target = PostgresOperator(
            task_id='delete_from_target',
            sql=f'''
            DELETE FROM {pg_target_schema}.{pg_target_table}
            WHERE 
                date IN (
                    SELECT 
                        date 
                    FROM 
                        {pg_tmp_schema}.{pg_tmp_table}
                    WHERE
                        kpi_id = {index_kpi}
                    )
            AND kpi_id = {index_kpi}
            ''',
            postgres_conn_id=pg_connect
        )

        insert_from_tmp_to_target = PostgresOperator(
            task_id='insert_from_tmp_to_target',
            sql=f'''
            INSERT INTO {pg_target_schema}.{pg_target_table}("date", value, kpi_id)
            SELECT 
                "date", 
                value, 
                {index_kpi} AS kpi_id 
            FROM 
                {pg_tmp_schema}.{pg_tmp_table}
            ''',
            postgres_conn_id=pg_connect
        )

        drop_tmp_after = PostgresOperator(
            task_id='drop_tmp_after',
            sql=f'''DROP TABLE IF EXISTS {pg_tmp_schema}.{pg_tmp_table}''',
            postgres_conn_id=pg_connect
        )

        end = EmptyOperator(
            task_id='end',
        )

        start >> sensors_task >> drop_tmp_before >> create_tmp >> delete_from_target >>\
        insert_from_tmp_to_target >> drop_tmp_after >> end

    return dag


# build a dag from dag config
def generator_of_morning_kpi_dag_to_gp() -> None:
    """
    Функция получает список config из БД и генерирует DAG's на основании функции `generator_of_morning_kpi_dag_to_gp`.

    Итерируется по config и каждый раз выполняет функцию `create_kpi_of_dag`, которая возвращает DAG.

    @return: None
    """
    pg_hook = PostgresHook(postgres_conn_id='test_db')

    df = pg_hook.get_pandas_df(  # noqa: PD901
        '''
        SELECT 
            kpi_id,
            dag_id,
            "owner",
            sql_query,
            start_date,
            pg_environment,
            airflow_environment,
            short_description_md,
            long_description_md,
            cron,
            sensors,
            tags
        FROM 
            dim_kpi_dag_gen_config
        WHERE 
            is_actual IS TRUE 
        ORDER BY 
            id;
        '''
    )

    for i in range(len(df)):
        create_kpi_of_dag(
            owner=df.iloc[i].owner,
            dag_id=df.iloc[i].dag_id,
            pg_target_schema='public',
            pg_target_table='dim_kpi_dag_gen_config',
            index_kpi=df.iloc[i].kpi_id,
            pg_environment=df.iloc[i].pg_environment,
            airflow_environment=df.iloc[i].airflow_environment,
            long_description=df.iloc[i].long_description_md,
            short_description=df.iloc[i].short_description_md,
            start_date=df.iloc[i].start_date,
            cron_expression=df.iloc[i].cron,
            tags=df.iloc[i].tags,
            sensors=df.iloc[i].sensors,
            sql_query=df.iloc[i].sql_query,
        )


generator_of_morning_kpi_dag_to_gp()
