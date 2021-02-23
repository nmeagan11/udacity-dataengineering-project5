from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    '''
    Loads JSON files from S3 to Amazon Redshift.
    
    Inputs:
    redshift_conn_id = connection to Redshift database
    aws_credentials_id = AWS credentials
    table = target table
    s3_bucket = S3 bucket
    s3_key = S3 key
    json = auto
    '''
    
    ui_color = '#358140'
    
    #SQL COPY
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="", 
                 s3_bucket="", 
                 s3_key="",
                 json="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):
        
        self.log.info('StageToRedshiftOperator: set Redshift connection')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('StageToRedshiftOperator: delete from Redshift')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('StageToRedshiftOperator: link to S3')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info('StageToRedshiftOperator: copy data from S3 to Redshift')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)

        self.log.info('StageToRedshiftOperator: complete')