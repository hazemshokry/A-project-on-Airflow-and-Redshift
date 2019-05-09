from datetime import datetime, timedelta
import os
from airflow import DAG
from helpers import SqlQueries
from subdags import load_staging_tables

from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Hazem Sayed',
    'retries':3,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

dag = DAG('Airflow_Redshift',
          default_args=default_args,
          start_date = datetime(2019, 1, 12),
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
          max_active_runs=1,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = SubDagOperator(
    subdag=load_staging_tables(
        'Airflow_Redshift',
        'Stage_events',
        'redshift',
        'aws_credentials',
        's3://dend/log_data',
        'public.staging_events',
        'CSV',
        '',
        "DELIMITER ',' IGNOREHEADER 1",
        SqlQueries.create_staging_events_SQL
),
    task_id = "Stage_events",
    dag = dag
)
    

stage_songs_to_redshift = SubDagOperator(
    subdag=load_staging_tables(
        'Airflow_Redshift',
        'Stage_songs',
        'redshift',
        'aws_credentials',
        's3://dend/song_data',
        'public.staging_songs',
        'JSON',
        "'auto'",  
        '',
        SqlQueries.create_staging_songs_SQL
),
    task_id = "Stage_songs",
    dag = dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift