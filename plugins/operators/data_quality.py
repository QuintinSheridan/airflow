from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 test_for_rows = [],
                 test_sql = [],
                 test_results = [],
                 retries = 0,
                 *args, 
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.test_for_rows = test_for_rows
        self.test_sql = test_sql
        self.test_results = test_results
        self.retries = retries

    def execute(self, context):
        self.log.info('Connecting to DB...')
        # connect to db
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Testing for table rows...')
        # test for rows
        for sql in self.test_for_rows:
            self.log.info(f'Test SQL: {sql}')
            # try up to self.retries times
            tests_passed = False
            
            for i in range(self.retries + 1):
                try:
                    result = redshift_hook.get_records(sql)[0][0]
                    self.log.info(result)
                    if result <= 0:
                        raise Exception()
                        
                    tests_passed = True
                    break
                    
                except:
                    self.log.info(f'Table missing rows: {sql}')
                    
            if not tests_passed:
                self.log.info('Tests Not passed')
                raise ValueError('SQL tests failed')
                
        self.log.info('running additional test...')
        # additional tests
        for i in range(len(self.test_sql)):
            sql = self.test_sql[i]
            self.log.info(f'Test SQL: {sql}')
            # try up to self.retries times
            tests_passed = False
            
            for i in range(self.retries + 1):
                try:
                    result = redshift_hook.get_records(sql)[0][0]
                    self.log.info(result)
                    if result != self.test_results[i]:
                        raise Exception()
                        
                    tests_passed = True
                    break
                    
                except:
                    self.log.info(f'Table missing rows: {sql}')
                    
            if not tests_passed:
                self.log.info('Tests Not passed')
                raise ValueError('SQL tests failed')
                
         
                
                
         
                