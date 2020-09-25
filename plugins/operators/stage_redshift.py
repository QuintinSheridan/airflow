from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 s3_storage_location='',
                 table = '', 
                 create_table = '',
                 s3 = '',
                 json_path = '',
                 aws_key = '',
                 aws_secret = '',
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.s3_storage_location = s3_storage_location
        self.table = table 
        self.create_table = create_table
        self.s3 = s3
        self.json_path = json_path
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator....')
        self.log.info('Connecting to Database...')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # create table
        self.log.info(f'Creating table {self.table}')
        redshift_hook.run(self.create_table)
        
        #copy from s3 to table
        copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            region 'us-west-2' compupdate off
            JSON {};
        """.format(self.table, self.s3, self.aws_key, self.aws_secret, self.json_path)
        
        self.log.info(f'Copying Data: {copy_sql}')
        redshift_hook.run(copy_sql)
        
        

       