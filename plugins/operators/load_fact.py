from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    '''
    Uses SQL to run data transformations to insert data into facts table.
    
    Inputs:
    redshift_conn_id = connection to Redshift database
    table_name = table name
    sql_query = SQL statement
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query

    def execute(self, context):
        
        self.log.info('LoadFactOperator: set Redshift connection')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('LoadFactOperator: execute SQL query')
        insert_statement = f'INSERT INTO {self.table_name} {self.sql_query}'
        redshift_hook.run(insert_statement)
        
        self.log.info('LoadFactOperator: complete')