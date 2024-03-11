import pandas as pd
import psycopg2
import mysql.connector
import json
import os



def postgres_conn():

    script_directory = os.path.dirname(__file__)
    file_path = os.path.join(script_directory, 'postgres_credentials.json')

    with open(file_path, 'r') as file1:
        config_postgres = json.load(file1)
        
    pg_conn = psycopg2.connect(
        database = config_postgres['database'],
        user = config_postgres['user'],
        password = config_postgres['password'],
        host= config_postgres['host'],
        port= config_postgres['port']
)       
    return pg_conn

def mysql_conn():
    
    script_directory = os.path.dirname(__file__)
    file_path1 = os.path.join(script_directory, 'mysql_credentials.json')

    with open(file_path1, 'r') as file2:
        config_mysql= json.load(file2)
        
    mysql_conn = mysql.connector.connect(
        user = config_mysql['user'],
        password = config_mysql['password'],
        host = config_mysql['host'],
        database = config_mysql['database']
)
    return mysql_conn


def Extract(pg_conn, table_name):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f'SELECT * FROM "{table_name}";')
    data = pg_cursor.fetchall()
    nombres_columnas = [columna[0] for columna in pg_cursor.description]
    df= pd.DataFrame(data, columns=nombres_columnas)

    return df






