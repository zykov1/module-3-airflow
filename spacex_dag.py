from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

rocket_types = ["falcon1", "falcon9", "falconheavy", "all"]
t1 = []
t2 = []

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")

for r in rocket_types:
    if r == "all":
        rpar = ""
    else:
        rpar = "-r %s" % r
    t1.append(BashOperator(
        task_id="get_sx_data_%s" % r,
        bash_command="python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} %s -o /var/data" % rpar,
        dag=dag
    ))
    t2.append(BashOperator(
        task_id="print_sx_data_%s" % r,
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv >> /var/data/opa",
        params={"rocket": r},
        dag=dag
    ))
    t1[-1] >> t2[-1]
