from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


# Defining the plugin class
class PtilePlugin(AirflowPlugin):
    name = "ptile_plugin"
    operators = [
        operators.VerticaToFileOperator,
        operators.FileToOracleOperator
    ]
