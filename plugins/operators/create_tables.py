from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    
    '''
    Creates initial table definitions in Redshift.
    
    Inputs:
    redshift_conn_id = connection to Redshift database
    '''
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        self.log.info('CreateTablesOperator: set Redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('CreateTablesOperator: run create_tables.sql')
        sql = open('/home/workspace/airflow/create_tables.sql','r').read()
        redshift.run(sql)

        self.log.info('CreateTablesOperator: complete')