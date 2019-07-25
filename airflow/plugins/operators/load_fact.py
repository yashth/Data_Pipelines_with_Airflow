from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """ INSERT INTO {} {}; COMMIT; """
    

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 aws_credentials="",
                 table="",
                 create_stmt="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.create_stmt = create_stmt
        self.load_sql_stmt = load_sql_stmt
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        

    def execute(self, context):
        self.log.info('LoadFactOperator implemented')
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        
        self.log.info('Create Table')
        redshift.run(self.create_stmt)
        
        self.log.info('Load Table')
        formatted_sql = LoadFactOperator.insert_sql.format( self.table, self.load_sql_stmt ) 
        redshift.run(formatted_sql)
        
        
        
       
        
