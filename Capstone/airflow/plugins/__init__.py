from airflow.plugins_manager import AirflowPlugin
import operators

class ImmigrationsPlugin(AirflowPlugin):
    name = 'immigrations_plugin'
    operators = [
             operators.CreateS3BucketOperator,
             operators.CopyFilesToS3Operator
    ]
