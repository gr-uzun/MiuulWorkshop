from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'miuul',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('02_download_data', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    wait_install_kaggle = ExternalTaskSensor(task_id='wait_install_kaggle',
                                             external_dag_id='01_install_kaggle',
                                             external_task_id='cat_credential',
                                             dag=dag)

    download_data_1 = BashOperator(task_id='download_data_1',
                                 bash_command='kaggle datasets download -p /opt/airflow/datasets --unzip fivethirtyeight/uber-pickups-in-new-york-city')

    download_data_2 = BashOperator(task_id='download_data_2',
                                 bash_command='kaggle datasets download -p /opt/airflow/datasets --unzip ravi72munde/uber-lyft-cab-prices')
    
    list_datasets = BashOperator(task_id='list_datasets', bash_command='ls -l /opt/airflow/datasets', retries=2)

    wait_install_kaggle >> download_data_1 >> download_data_2 >> list_datasets
