from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 17, 20, 04, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'git_sync',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

git_pull = BashOperator(
    task_id='git_clone',
    bash_command='git -C /root/airflow/dags/ pull',
    dag=dag,
)

pwd = BashOperator(
    task_id='pwd',
    bash_command='pwd',
    dag=dag,
)
