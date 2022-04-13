

from email.policy import default
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago
from airflow.models import taskinstance, variable
from datetime import datetime, timedelta
import sys, os

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2022, 4, 10),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'depends_on_past' : False,
}

dag = DAG(
    dag_id='steam_review_extraction',
    default_args=default_args,
    catchup=False
)

command = f"""python3 /home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/kafka/producer.py"""

t_extract_reviews = BashOperator(
    task_id='t_extract_reviews',
    bash_command=command,
    dag=dag

)

# Task sequence

t_extract_reviews