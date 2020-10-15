import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (VerticaToFileOperator, FileToOracleOperator)
from sql import SqlQueries
from helpers import compute_ptile

default_args = {
    'owner': 'md5822',
    'start_date': datetime(2020, 9, 27),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# Hello world function to test task and dag
def hello_world(*args, **kwargs):
    logging.info("HELLO WORLD!")
    logging.info(args)
    logging.info(kwargs)

    
#Check if there is any data
def check_not_zero(file, *args, **kwargs):
    df = pd.read_csv(file)
    if (!df.shape[0] > 0):
        raise('No data')

        
# Check if both vendor queries were pulled successfully
def check_vendor_count(file, *args, **kwargs):
    df = pd.read_csv(file)
    if (!len(df['VENDOR'].unique()) == 2):
        raise('Missing 1 or more vendor data')


dag = DAG('dag_ptile',
          default_args=default_args,
          description='Collect RAN pm counters and computes 5th percentile throughput KPIs')

dag_skip = DAG('dag_skip',
               default_args=default_args,
               description='tasks to skip')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

test_task = PythonOperator(
    task_id="Test",
    provide_context=True,
    python_callable=hello_world,
    dag=dag_skip
)

vertica_to_file = VerticaToFileOperator(
    task_id="Vertica_to_file",
    provide_context=True,
    vertica_conn_id="v_prod",
    sql=[SqlQueries.eri_sql, SqlQueries.nok_sql],
    dag=dag_skip
)

file_transform = PythonOperator(
    task_id="Compute_ptile",
    provide_context=True,
    python_callable=compute_ptile,
    op_kwargs={'in_file': 'ran_raw_2020-09-30.csv.csv', 'out_file': 'sample_output.csv'},
    dag=dag
)

data_check_1 = PythonOperator(
    task_id="Data_check_1",
    python_callable=check_not_zero,
    op_kwargs={'file': 'ran_raw_2020-09-30.csv.csv'},
    dag=dag
)

data_check_2 = PythonOperator(
    task_id="Data_check_2",
    python_callable=check_vendor_count,
    op_kwargs={'file': 'ran_raw_2020-09-30.csv.csv'},
    dag=dag
)

file_to_oracle = FileToOracleOperator(
    task_id="File_to_oracle",
    provide_context=True,
    oracle_conn_id="oracle_idaho",
    dag=dag_skip
)

# Production DAG
# start_operator >> vertica_to_file >> file_transform >> 
#     data_check_1 >> data_check_2 >> file_to_oracle
    
# Modified DAG for this project due to AT&T proprietary databases in 
# Vertica_to_file and File_to_oracle tasks
start_operator >>  file_transform >> data_check_1 >> data_check_2 
