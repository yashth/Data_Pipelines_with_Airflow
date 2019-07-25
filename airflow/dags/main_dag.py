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
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag, )


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    table="staging_events",
    conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-12-events.json",
    create_stmt=SqlQueries.create_table_staging_events
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/B/C/TRABCEI128F424C983.json",
    create_stmt=SqlQueries.create_table_staging_songs
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    conn_id="redshift",
    create_stmt = SqlQueries.create_table_songplays,
    load_sql_stmt = SqlQueries.songplays_table_insert,
    aws_credentials="aws_credentials",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    conn_id="redshift",
    create_stmt = SqlQueries.create_table_users,
    load_sql_stmt = SqlQueries.user_table_insert,
    aws_credentials="aws_credentials",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    conn_id="redshift",
    create_stmt = SqlQueries.create_table_songs,
    load_sql_stmt = SqlQueries.song_table_insert,
    aws_credentials="aws_credentials",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    conn_id="redshift",
    create_stmt = SqlQueries.create_table_artists,
    load_sql_stmt = SqlQueries.artist_table_insert,
    aws_credentials="aws_credentials",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    conn_id="redshift",
    create_stmt = SqlQueries.create_table_time,
    load_sql_stmt = SqlQueries.time_table_insert,
    aws_credentials="aws_credentials",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table="users",
    aws_credentials="aws_credentials",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator