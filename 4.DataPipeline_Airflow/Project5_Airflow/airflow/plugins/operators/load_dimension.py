from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    DAG operator used to insert data into dimension table.
    :param string  redshift_conn_id: Reference to a specific redshift database
    :param string  table: Redshift table to insert
    :param string  sql: Sql statement to insert data into fact table
    :param boolean  overwrite: Set true to overwrite table (Default False)
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 overwrite=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.overwrite = overwrite
        
    def execute(self, context):
        self.log.info(f"LoadDimensionOperator with table: {self.table}")
        
        if self.overwrite:
            self.log.info(f"Truncating table: {self.table}")
            sql_trunc = f"TRUNCATE TABLE {self.table}"
            redshift_hook.run(sql_trunc)
        
        sql = f"""
            INSERT INTO {self.table} 
            {self.sql}"""
        redshift.run(sql)
