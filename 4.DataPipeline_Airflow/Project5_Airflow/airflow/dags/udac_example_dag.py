from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup' : False,
    'schedule_interval': '@hourly'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id=getRedshiftConnId(),
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    extra_params="json 'auto' compupdate off region 'us-west-2'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    provide_context=True,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    dag=dag

)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    provide_context=True,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    overwrite=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    overwrite=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    overwrite=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    overwrite=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tbl_col={'songs': 'songid', 
             'artists': 'artistid', 
             'users': 'userid'}
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG Task Dependencies
# DAG: Stage 0
start_operator >> create_tables_task

# DAG: Stage 1
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

# DAG: Stage 2
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# DAG: Stage 3
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# DAG: Stage 4
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# DAG: Stage 5
run_quality_checks >> end_operator

