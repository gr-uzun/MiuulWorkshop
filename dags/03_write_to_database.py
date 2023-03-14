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
            print(filename.lower().replace('-', '_').split(".")[0])
            df = pd.read_csv(os.path.join(dirname, filename), encoding='latin-1')
            print(df.head())


def write_to_postgres(**kwargs):
    for dirname, _, filenames in os.walk(kwargs['path']):
        for filename in filenames:
            table_name = filename.lower().replace('-', '_').split(".")[0]
            print(table_name)
            df = pd.read_csv(os.path.join(dirname, filename), encoding='latin-1')
            df.dropna(inplace=True, axis=1)
            print(df.head())
            try:
                df.to_sql(name=table_name, con=kwargs['engine'], if_exists='replace')
            except:
                for i in range(0, len(df), 10000):
                    if i == 0:
                        df.loc[i: i + 10000].to_sql(name=table_name, con=kwargs['engine'],
                                                    if_exists='replace')
                    else:
                        df.loc[i: i + 10000].to_sql(name=table_name, con=kwargs['engine'],
                                                    if_exists='append')


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
