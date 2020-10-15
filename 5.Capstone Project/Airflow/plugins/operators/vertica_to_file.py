from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd


class VerticaToFileOperator(BaseOperator):
    """
    DAG operator to extract data from vertica and write to flat file
    :param string  vertica_conn_id: reference to a specific vertica database
    :param string or list[string]  sql: SQL statement(s) to be executed and concatenated
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 vertica_conn_id="",
                 sql="",
                 *args, **kwargs):
        super(VerticaToFileOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.sql = sql

    def execute(self, context):
        vertica_hook = VerticaHook(self.vertica_conn_id)
        start_date = context.get("execution_date") - timedelta(days=2)
        end_date = start_date + timedelta(days=1)
        if type(self.sql) is not list:
            self.sql = [self.sql]
        dfs = []
        for query in self.sql:
            df = vertica_hook.get_pandas_df(query.format(start_date=start_date, end_date=end_date))
            dfs.append(df)
        result = pd.concat(dfs)
        result.to_csv('test_output2.csv')
        result.to_csv("ran_raw_{}.csv.gz".format(start_date), compression='gzip')
        self.log.info(f"Dataframe shape: {result.shape}")




