from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'miuul',
    'start_date': yesterday_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('01_install_kaggle', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    upgrade_pip = BashOperator(task_id='upgrade_pip',
                               bash_command="""python -m pip install --upgrade pip""")

    install_kaggle = BashOperator(task_id='install_kaggle', bash_command='pip install kaggle')

    mkdir_kaggle = BashOperator(task_id='mkdir_kaggle',
                                bash_command="""if [ -d  ~/.kaggle ];
                        then rm -rf  ~/.kaggle;
                        else mkdir  ~/.kaggle;
                        fi;""")

    kaggle_credential = BashOperator(task_id='kaggle_credential',
                                     bash_command="""cat <<EOF >  ~/.kaggle/kaggle.json
                                    {"username":"kayademirs","key":"52bdba8b495319b8f21448584ccee663"}
                                    EOF
                                    """)

    chmod_credential = BashOperator(task_id='chmod_credential',
                                    bash_command="""chmod 600 ~/.kaggle/kaggle.json""",
                                    retries=2)

    cat_credential = BashOperator(task_id='cat_credential', bash_command='cat ~/.kaggle/kaggle.json')

    upgrade_pip >> install_kaggle >> mkdir_kaggle >> kaggle_credential >> chmod_credential >> cat_credential
