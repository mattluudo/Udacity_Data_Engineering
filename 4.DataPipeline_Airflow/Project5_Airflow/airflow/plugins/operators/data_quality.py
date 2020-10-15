from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    DAG operator that checks for null values in tables
    :param string  redshift_conn_id: reference to a specific redshift database
    :param dictionary  tbl_col: table and column key pairs to check for nulls
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tbl_col="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tbl_col = tbl_col

    def execute(self, context):
        self.log.info('DataQualityOperator running...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fail_cnt = 0
        for tbl, col in tbl_col.items():
            sql_check = f"SELECT COUNT(*) FROM {tbl} WHERE {col} is null"
            result = redshift.get_records(sql)[0]
            if result != 0:
                self.log.info(f"Table {tbl} has null values in column {col}")
                fail_cnt += 1
        if fail_cnt > 0:
            raise ValueError('Null value found in table(s). Check log for details')
        
        self.log.info('DataQualityOperator complete!')