from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_load_stmt="",
                 target_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.target_table = target_table
        self.sql_load_stmt = sql_load_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id='redshift')

        self.log.info(f"Loading {self.target_table} table in Redshift")
        self.log.info(print(self.redshift_conn_id))
        redshift.run(self.sql_load_stmt)
