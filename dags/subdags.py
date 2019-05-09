from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
#import sql

def load_staging_tables(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    aws_credentials_id,
    s3_path,
    target_table,
    data_type,
    data_format,
    additional_paramaters,
    sql_stmt,
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
    