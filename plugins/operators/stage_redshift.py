from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("execution_time",)
    
    copy_staging = """
    copy {} from '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    format as {} {} {} compupdate off TRUNCATECOLUMNS region 'us-west-2';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_path = "",
                 target_table="",
                 data_type="",
                 data_format="",
                 additional_paramaters="",
                 execution_time="{{ ts }}",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.target_table = target_table
        self.execution_time = execution_time
        self.data_type=data_type
        self.data_format = data_format
        self.additional_paramaters = additional_paramaters

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        self.log.info('Copying data from S3 to Staging tables in Redshift')
        redshift.run(StageToRedshiftOperator.copy_staging.format(self.target_table,
                                               self.s3_path,
                                               credentials.access_key,
                                               credentials.secret_key,
                                               self.data_type,
                                               self.data_format,
                                               self.additional_paramaters))
        





