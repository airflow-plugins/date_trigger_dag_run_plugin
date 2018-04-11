from airflow.plugins_manager import AirflowPlugin
from DateTriggerDagRunPlugin.operators.date_trigger_dag_run_operator import DateTriggerDagRunOperator


class DateTriggerDagRunPlugin(AirflowPlugin):
    name = "date_trigger_dag_run_plugin"
    operators = [DateTriggerDagRunOperator]
