from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator, DataQualityOperator)
#import sql

def load_staging_tables(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    s3_path,
    target_table,
    data_type,
    sql_stmt,
    data_format = "",
    additional_paramaters = "",
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        start_date = datetime(2019, 1, 12),
        **kwargs
    )
    
    create_task = PostgresOperator(
        task_id=f"create_{target_table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_stmt
    )
    
    load_task = StageToRedshiftOperator(
    task_id=f"load_{target_table}_table",
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id=aws_credentials_id,
    target_table=target_table,
    data_type=data_type,
    data_format=data_format,
    additional_paramaters=additional_paramaters,
    s3_path=s3_path,
    dag=dag
    )
    
    create_task >> load_task
    
    return dag
    
def load_dim_tables (
    parent_dag_name,
    task_id,
    redshift_conn_id,
    target_table,
    sql_create_stmt,
    sql_load_stmt,
    insert_mode,
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        start_date = datetime(2019, 1, 12),
        **kwargs
    )

    if (insert_mode == 0):
        sql_load_stmt = "DROP TABLE IF EXISTS " + target_table + "; " + sql_create_stmt + sql_load_stmt
    else:
        pass

    
    create_task = PostgresOperator(
        task_id=f"create_{target_table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_create_stmt
    )

    load_task = LoadDimensionOperator(
        task_id=f"load_{target_table}_table",
        provide_context=True,
        sql_load_stmt = sql_load_stmt,
        redshift_conn_id=redshift_conn_id,
        target_table=target_table,
        dag=dag
    )

    create_task >> load_task

    return dag

def data_quality_check (
    parent_dag_name,
    task_id,
    redshift_conn_id,
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        start_date = datetime(2019, 1, 12),
        **kwargs
    )

    users_check_task = DataQualityOperator (
        task_id="check_public.users_quality",
        redshift_conn_id=redshift_conn_id,
        dag = dag,
        table = "public.users"
    )

    artists_check_task = DataQualityOperator (
        task_id="check_public.artists_quality",
        redshift_conn_id=redshift_conn_id,
        dag = dag,
        table = "public.artists"
    )

    songs_check_task = DataQualityOperator (
        task_id="check_public.songs_quality",
        redshift_conn_id=redshift_conn_id,
        dag = dag,
        table = "public.songs"
    )

    times_check_task = DataQualityOperator (
        task_id="check_public.times_quality",
        redshift_conn_id=redshift_conn_id,
        dag = dag,
        table = "public.times"
    )

    users_check_task >> artists_check_task >> songs_check_task >> times_check_task

    return dag
