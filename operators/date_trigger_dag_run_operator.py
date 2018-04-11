from datetime import datetime
import logging

from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class DateTriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` if a criteria is met

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = ['validation_date']
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            validation_date,
            trigger_date,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.validation_date = validation_date
        self.trigger_date = trigger_date

    def execute(self, context):
        dro = DagRunOrder(run_id='trig__' + datetime.now().isoformat())

        if (datetime
           .strptime(self.validation_date,
                     '%Y-%m-%d')
           .strftime('%A') in self.trigger_date):
            session = settings.Session()
            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(self.trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id,
                state=State.RUNNING,
                conf=dro.payload,
                external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")

        date = {'validation_date': self.validation_date}

        return date
