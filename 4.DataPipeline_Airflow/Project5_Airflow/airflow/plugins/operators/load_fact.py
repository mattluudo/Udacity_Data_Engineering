from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    DAG operator used to insert data into fact table.
    :param string  redshift_conn_id: reference to a specific redshift database
    :param string  table: redshift table to insert
    :param string  sql: sql statement to insert data into fact table
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql


    def execute(self, context):
        self.log.info(f"LoadFactOperator with table: {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = f"""
            INSERT INTO {self.table} 
            {self.sql}"""
        redshift.run(sql)

        