import random
import pandas as pd
from connectors_to_databases import PostgreSQL

pg = PostgreSQL(port=1)

pg.execute_script(
    '''
    CREATE TABLE IF NOT EXISTS fct_some_table_with_random_values
    ("date" date, "values" int8)
    '''
)

date_list = list(pd.date_range(start='2022-01-01', end='2024-01-01'))

values_list = [random.randint(a=1,b=100) for i in range(len(date_list))]

dict_ = {
    'date': date_list,
    'values': values_list
}

df = pd.DataFrame(dict_)

pg.insert_df(df=df, table_schema='public', table_name='fct_some_table_with_random_values',)