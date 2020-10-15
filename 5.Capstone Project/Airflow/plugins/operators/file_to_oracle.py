from airflow.hooks.oracle_hook import OracleHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def dateparse(dt_str):
    return datetime.strptime(dt_str, '%m/%d/%Y')


class FileToOracleOperator(BaseOperator):
    """
    DAG operator to extract data from vertica and write to flat file
    :param string  vertica_conn_id: reference to a specific vertica database
    :param string or list[string]  sql: SQL statement(s) to be executed and concatenated
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 oracle_conn_id="",
                 *args, **kwargs):
        super(FileToOracleOperator, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.table = table
        self.file = file

    def execute(self, context):
        oracle_hook = OracleHook(self.oracle_conn_id)
        start_date = context.get("execution_date") - timedelta(days=2)
        file = self.file
        df = pd.read_csv(file)
        lol = df.values.tolist()
        for li in lol:
            li[0] = dateparse(li[0])
        rows = [tuple(l) for l in lol]
        oracle_hook.insert_rows(
           table=self.table,
           rows=rows,
           commit_every=1000)
        self.log.info(f"start date: {start_date}")




