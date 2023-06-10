import os
import time
from datetime import datetime, timedelta

import pandas as pd

pd.set_option('display.max_columns', None)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from sqlalchemy import create_engine

# PostgreSQL veritabanı motorunu oluşturma
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/postgres')


# Dosyaları okuma işlemini gerçekleştiren fonksiyon
def read_files(**kwargs):
    for dirname, _, filenames in os.walk(kwargs['path']):
        for filename in filenames:
            if filename.split(".")[-1] == "csv":
                print(filename.lower().replace('-', '_').split(".")[0])
                df = pd.read_csv(os.path.join(dirname, filename), encoding='latin-1', low_memory=True,
                                 nrows=1000)
                print(df.head())


# Tarih/saat formatlama işlemini gerçekleştiren fonksiyon
def datetime_format(df):
    try:
        if 'date/time' in df.columns:
            df['datetime_new'] = pd.to_datetime(df["date/time"], format='%m/%d/%Y %H:%M:%S').dt.strftime(
                '%d/%m/%Y %H:%M:%S')
        if 'date' in df.columns:
            df['date_new'] = pd.to_datetime(df["date"], format='%m/%d/%Y').dt.strftime('%d/%m/%Y')
        if 'time_stamp' in df.columns:
            df['time_stamp_new'] = pd.to_datetime(df['time_stamp'])
    except:
        if 'date' in df.columns:
            df['date_new'] = pd.to_datetime(df["date"], format='%Y.%m.%d').dt.strftime('%d/%m/%Y')
    return df


# Verileri PostgreSQL'e yazma işlemini gerçekleştiren fonksiyon
def write_to_postgres(**kwargs):
    for dirname, _, filenames in os.walk(kwargs['path']):
        for filename in filenames:
            if filename.split(".")[-1] == "csv":
                print("filename: {}".format(filename))
                table_name = filename.lower().replace('-', '_').split(".")[0]
                df = pd.read_csv(os.path.join(dirname, filename), low_memory=True,
                                 nrows=100000, encoding='latin-1')
                df.columns = [c.lower() for c in df.columns]
                df.dropna(inplace=True, how='all', axis=1)
                df = datetime_format(df)
                print(df.head())
                time.sleep(5)
                df.to_sql(name=table_name, con=kwargs['engine'], if_exists='replace')
                print("{} table created".format(table_name))
    print("############################## Data Transfer is successful! ###############################".upper())


# Dünün tarihini alarak formatlayın
yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

# Task'lerin varsayılan argümanları
default_args = {
    'owner': 'miuul',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# DAG oluşturma
with DAG('03_write_to_postgresql', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:
    # '02_download_data' DAG'ındaki 'list_datasets' task'ının tamamlanmasını bekleyen sensor
    wait_download_data = ExternalTaskSensor(task_id='wait_download_data',
                                            external_dag_id='02_download_data',
                                            external_task_id='list_datasets',
                                            dag=dag)

    # Tüm veri setlerini okuyan task
    read_all_dataset = PythonOperator(task_id='read_file', python_callable=read_files,
                                      op_kwargs={'path': '/opt/airflow/datasets/'})

    # Verileri PostgreSQL'e yazan task
    write_data = PythonOperator(task_id='write_data', python_callable=write_to_postgres,
                                op_kwargs={'path': '/opt/airflow/datasets/',
                                           'engine': engine})

    # Task'ler arasındaki bağıntıları belirtme
    wait_download_data >> read_all_dataset >> write_data
