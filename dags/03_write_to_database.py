import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/postgres')


def read_files(**kwargs):
    for dirname, _, filenames in os.walk(kwargs['path']):
        for filename in filenames:
            if filename.split(".")[-1] == "csv":
                print(filename.lower().replace('-', '_').split(".")[0])
                df = pd.read_csv(os.path.join(dirname, filename), encoding='latin-1', low_memory=True,
                                 nrows=1000)
                print(df.head())


def datetime_format(df):
    try:
        if 'date/time' in df.columns:
            df['date/time'] = pd.to_datetime(df["date/time"], format='%m/%d/%Y %H:%M:%S')
            df['date/time'] = df["date/time"].dt.strftime('%d/%m/%Y %H:%M:%S')
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df["date"], format='%m/%d/%Y')
            df['date'] = df["date"].dt.strftime('%d/%m/%Y')
    except:
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df["date"], format='%Y.%m.%d')
            df['date'] = df["date"].dt.strftime('%d/%m/%Y')
    return df


def write_to_postgres(**kwargs):
    for dirname, _, filenames in os.walk(kwargs['path']):
        for filename in filenames:
            if filename.split(".")[-1] == "csv":
                print("filename: {}".format(filename))
                table_name = filename.lower().replace('-', '_').split(".")[0]
                df = pd.read_csv(os.path.join(dirname, filename), encoding='latin-1', low_memory=True,
                                 nrows=100000)
                df.columns = [c.lower() for c in df.columns]
                df.dropna(inplace=True, axis=1)
                df = datetime_format(df)
                df.to_sql(name=table_name, con=kwargs['engine'], if_exists='replace')
                print("{} table created".format(table_name))
    print("############################## Data Transfer is successful! ###############################".upper())


yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'miuul',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('03_write_to_postgresql', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:
    wait_download_data = ExternalTaskSensor(task_id='wait_download_data',
                                            external_dag_id='02_download_data',
                                            external_task_id='list_datasets',
                                            dag=dag)

    read_all_dataset = PythonOperator(task_id='read_file', python_callable=read_files,
                                      op_kwargs={'path': '/opt/airflow/datasets/'})

    write_data = PythonOperator(task_id='write_data', python_callable=write_to_postgres,
                                op_kwargs={'path': '/opt/airflow/datasets/',
                                           'engine': engine})

    wait_download_data >> read_all_dataset >> write_data
