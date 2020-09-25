from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 create_table = '',
                 insert_table = '',
                 insert_select = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.insert_table = insert_table
        self.insert_select = insert_select

        
    def execute(self, context):
        self.log.info('FUCK LoadFactOperator not implemented yet')
        self.log.info(f'create_table: {self.create_table}')
        self.log.info(f'insert_query: {self.insert_select}')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # create table
        redshift_hook.run(self.create_table)
        
        insert_query = 'INSERT INTO {table} ({query})'.format(table=self.insert_table, query=self.insert_select)
        self.log.info(f'insert_query: {insert_query}')
        
        # insert into facts table 
        redshift_hook.run(insert_query)