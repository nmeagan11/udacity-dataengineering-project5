from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    '''
    Uses SQL to run data transformations to insert data into dimension tables.
    
    Inputs:
    redshift_conn_id = connection to Redshift database
    table_name = table name
    sql_query = SQL statement
    delete_flag = if True, deletes from SQL table; appends otherwise
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_query="",
                 delete_flag=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query
        self.delete_flag = delete_flag

    def execute(self, context):
        
        self.log.info('LoadDimensionOperator: set Redshift connection')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_flag:
            self.log.info('LoadDimenstionOperator: running delete function')
            delete_statement = f'DELETE FROM {self.table_name}'
            redshift_hook.run(delete_statement)
        
        self.log.info('LoadDimensionOperator: execute SQL query')
        insert_statement = f'INSERT INTO {self.table_name} {self.sql_query}'
        redshift_hook.run(insert_statement)
        
        self.log.info('LoadDimensionOperator: complete')
