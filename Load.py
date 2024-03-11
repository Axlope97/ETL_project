from Transformation import transform
import pandas as pd
import mysql.connector


countries, paises, population, life_expectancy, infant_death_rate = transform()


countries.name = 'countries'
paises.name = 'paises'
population.name = 'population'
life_expectancy.name = 'life_expectancy'
infant_death_rate.name = 'infant_death_rate'

# Creating df list
list_df = [countries, paises, population, life_expectancy, infant_death_rate]

def Load():
    from Extraction import mysql_conn

    mysql_connection = mysql_conn()
    mysql_cursor = mysql_connection.cursor()

    for df in list_df:
        # Obtaining data name
        table_name = df.name

        if len(df.columns) == 0:
            raise ValueError(f"The DataFrame {table_name} has no columns.")

        # Taking data type and adding to columns
        column_types = {col: str(df[col].dtype) for col in df.columns}
        dtype_mapping = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)'}
        columns_sql = ', '.join([f'`{col}` {dtype_mapping.get(column_types[col], "VARCHAR(255)")} DEFAULT NULL' for col in df.columns])

        # check if table exist
        mysql_cursor.execute(f'SHOW TABLES LIKE \'{table_name}\'')
        table_exists = mysql_cursor.fetchone() is not None

        if not table_exists:
            # Creating table if doesn't exist
            mysql_cursor.execute(f'CREATE TABLE {table_name} ({columns_sql}) ENGINE=InnoDB;')

        # Loading values
        for index, row in df.iterrows():
            cols = ", ".join([f'`{col}`' for col in df.columns])
            values = ', '.join([f'"{row[col]}"' if df[col].dtype == 'O' else str(row[col]) for col in df.columns])

            mysql_cursor.execute(f'INSERT INTO {table_name} ({cols}) VALUES ({values});')

    mysql_connection.commit()
    mysql_cursor.close()
    mysql_connection.close()

Load()





