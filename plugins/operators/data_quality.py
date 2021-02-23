from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    '''
    Checks count of each table to ensure data quality.
    
    Inputs:
    redshift_conn_id = connection to Redshift database
    table_name = list of table names
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        
        self.log.info('DataQualityOperator: set Redshift connection')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('DataQualityOperator: running checks')
        for table in self.table_names:
            result = redshift_hook.run(f'SELECT COUNT(*) FROM {table}')
            if result == 0:
                self.log.error('DataQualityOperator: data quality check FAILED for {table}')
                raise ValueError('DataQualityOperator: data quality check FAILED for {table}')
        self.log.info('DataQualityOperator: data quality check PASSED for {table}')
        
        self.log.info('DataQualityOperator: complete')