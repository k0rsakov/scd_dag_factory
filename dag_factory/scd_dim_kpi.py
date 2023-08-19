from connectors_to_databases import PostgreSQL


pg = PostgreSQL(
    port=1
)


TABLE = 'dim_kpi_dag_gen_config'


def gen_insert_sql_for_kpi_id(dict_kpi: dict = None) -> str:
    """
    Генерирует скрипт для вставки данных в SCD.

    Определяет есть ли такой ключ.
    Если нет, то делает вставку с нужными данными, указанными в dict_kpi.
    Если есть, то делает вставку с нужными данными, указанными в dict_kpi и дублирует информацию из прошлых строк.

    @param dict_kpi: Словарь с описанием kpi.
    @return: Строку для вставки значений в SCD.
    """

    # Проверка наличия kpi_id в таблице
    df_check = pg.execute_to_df(f'''
    SELECT
        kpi_id
    FROM
        {TABLE}
    WHERE
        kpi_id = {dict_kpi['kpi_id']}
    ''')

    # Проверяем есть ли такой kpi_id в таблице
    if len(df_check) >= 1:
        # В запросе исключаем те поля, которые генерируется сами через `DEFAULT`
        query = f'''
            SELECT 
                column_name 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = '{TABLE}'
                AND column_name NOT IN (
                    'id', 'created_at', 'changed_at', 'is_actual', 
                    {', '.join(f"'{i}'" for i in dict_kpi.keys())}
                )
        '''

        df = pg.execute_to_df(query)

        insert_sql_column_current = ', '.join(value for value in df.column_name)
        insert_sql_column_modified = insert_sql_column_current + f''', {', '.join(i for i in dict_kpi.keys())}'''

        list_values = []

        for value in dict_kpi.values():
            # Обработка одинарных кавычек в значениях. Они встречаются при указании дат.
            if "'" in str(value):
                value = value.replace("'", "''")
                list_values.append(f"'{value}'")
            elif value is None:
                list_values.append('NULL')
            else:
                list_values.append(f"'{value}'")

        insert_sql_column_values = insert_sql_column_current + f''', {', '.join(list_values)}'''

        sql_insert = f'''
        INSERT INTO {TABLE}
        (
            {insert_sql_column_modified}
        )
        SELECT 
            {insert_sql_column_values} 
        FROM 
            {TABLE}
        WHERE
            is_actual IS TRUE
            AND kpi_id = {dict_kpi['kpi_id']};
        '''
    else:
        # Если нет такого kpi_id в таблице, то генерируем вставку значений из словаря
        columns = ', '.join(value for value in dict_kpi.keys())

        list_values = []

        for value in dict_kpi.values():
            if "'" in str(value):
                value = value.replace("'", "''")
                list_values.append(f"'{value}'")
            elif value is None:
                list_values.append('NULL')
            else:
                list_values.append(f"'{value}'")

        values = ', '.join(list_values)

        sql_insert = f'''
            INSERT INTO {TABLE}({columns})
            VALUES ({values});
            '''

    return sql_insert


def scd_dim_kpi(dict_kpi: dict = None) -> None:
    """
    Основная функция, которая принимает на вход словарь с описанием kpi.

    Каждый ключ – это поле в таблице SCD.
    Каждое значение – это значение поля в таблице SCD.

    @param dict_kpi: Словарь с описанием kpi по выбранным колонкам.
    @return: Ничего не возвращает, выполняет SQL-скрипт на вставку данных в SCD.
    """

    # Обновление changed_at в предыдущей актуальной записи
    update_changed_at_for_kpi_id = f'''
    UPDATE {TABLE} 
    SET 
        changed_at = NOW() 
    WHERE 
        kpi_id = {dict_kpi['kpi_id']} 
        AND is_actual IS TRUE;
    '''

    # Вставка новой записи с обновленными значениями полей
    insert_new_values_for_kpi_id = gen_insert_sql_for_kpi_id(dict_kpi=dict_kpi)

    # Обновление is_actual для каждого kpi_id
    update_is_actual_for_kpi_id = f'''
    UPDATE {TABLE} 
    SET 
        is_actual = false 
    WHERE 
        kpi_id = {dict_kpi['kpi_id']} 
        AND id <> (
            SELECT MAX(id) 
            FROM {TABLE} 
            WHERE kpi_id = {dict_kpi['kpi_id']}
        );
    '''

    # Собираем SQL-скрипт из разных кусков, чтобы он прошел в одной транзакции
    sql_query = update_changed_at_for_kpi_id + insert_new_values_for_kpi_id + update_is_actual_for_kpi_id

    print(sql_query)
    pg.execute_script(sql_query)


# Пример использования
new_values = {
    'kpi_id': 1,
    'dag_id': 'test_1',
    'metric_name_en': 'test_1',
    'owner': 'korsak0v',
    'start_date': '2021-01-01',
    'cron': '10 0 * * *',
    'tags': '''['dm', 'pg', 'gen_dag', 'from_pg']''',
    'sql_query': '''
    SELECT
        date,
        count(values) AS value
    FROM
        fct_some_table_with_random_values
    WHERE
        date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        1
    ''',
}


# Вызов функции
scd_dim_kpi(
    dict_kpi=new_values
)
