from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 aws_credentials="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials

    def execute(self, context):
        self.log.info('DataQualityOperator implemented')
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Check Data Table')        
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
  
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed.{self.table} contained 0 results")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
            
            
            