from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    DAG operator to copy data from S3 into redshift staging tables.
    :param string  redshift_conn_id: reference to a specific redshift database
    :param string  aws_credentials_id: airflow connection reference to aws
    :param string  table: table to be staged
    :param string  s3_bucket: S3 bucket location
    :param string  s3_bucket: S3 path
    :param string  region: S3 location
    :param string  extra_params: Extra parameters needed in SQL statement
    """
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_path="",
                 region=""
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path= s3_path,
        self.region= region,
        self.extra_params = extra_params

    def execute(self, context):
        self.log.info(f"StageToRedshiftOperator with table: {self.table}")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql = f"""
            COPY {self.table} 
            FROM 's3://{self.s3_bucket}/{self.s3_path}' 
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            TIMEFORMAT as 'epochmillisecs'
            {self.extra_params}"""
        
        redshift_hook.run(sql)



