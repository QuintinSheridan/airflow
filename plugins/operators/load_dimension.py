from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 create_table = '',
                 insert_table = '',
                 insert_select = '',
                 append = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        
        self.redshift_conn_id = redshift_conn_id
        self.create_table = create_table
        self.insert_table = insert_table
        self.insert_select = insert_select
        self.append = append

        
    def execute(self, context):
        self.log.info('Connecting to DB....')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # create table
        self.log.info('Creating Table....')
        redshift_hook.run(self.create_table)
        insert_query = 'INSERT INTO {table} ({query})'.format(table=self.insert_table, query=self.insert_select)
        self.log.info(f'insert_query: {insert_query}')
        
        if self.append:
            redshift_hook.run(insert_query)
        else:
            delete_query = 'DELETE FROM {}'.format(self.insert_table)
            redshift_hook.run(delete_query)
            redshift_hook.run(insert_query)