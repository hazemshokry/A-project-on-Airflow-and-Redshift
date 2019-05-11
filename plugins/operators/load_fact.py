from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_create_stmt="",
                 sql_load_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_create_stmt=sql_create_stmt
        self.sql_load_stmt=sql_load_stmt


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Creating fact table in Redshift')
        redshift.run(self.sql_create_stmt)

        self.log.info('Loading fact table in Redshift')
        self.log.info(print(self.redshift_conn_id))
        redshift.run(self.sql_load_stmt)
