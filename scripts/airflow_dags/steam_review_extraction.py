from email.policy import default
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago
from airflow.models import taskinstance, variable
from datetime import datetime, timedelta
import sys, os

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/kafka')
from producer import fn_start_producer

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/spark/api')
from p_from_bronze_to_silver import fn_move_from_bronze_to_silver
from p_from_silver_to_gold import fn_move_from_silver_to_gold

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

t_extract_reviews = PythonOperator(
    task_id='t_extract_reviews',
    python_callable=fn_start_producer,
    dag=dag
)

t_move_to_silver = PythonOperator(
    task_id='t_move_to_silver',
    python_callable=fn_move_from_bronze_to_silver,
    dag=dag
)

t_move_to_gold = PythonOperator(
    task_id='t_move_to_gold',
    python_callable=fn_move_from_silver_to_gold,
    dag=dag
)
# Task sequence

t_extract_reviews >> t_move_to_silver >> t_move_to_gold