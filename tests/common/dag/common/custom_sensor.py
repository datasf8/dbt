from airflow.models import TaskInstance,DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from sqlalchemy import and_,desc
class CustomTaskSensor(ExternalTaskSensor):
    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            count_filtered = session.query(TaskInstance).join(DagRun,and_(TaskInstance.dag_id==DagRun.dag_id,TaskInstance.run_id==DagRun.run_id)).filter(
                TaskInstance.dag_id == self.external_dag_id,
                TaskInstance.task_id == self.external_task_id
            ).order_by(DagRun.execution_date.desc()).first()

            if count_filtered.state in self.allowed_states:
                return True
            else:
                return False
        else:
            return super(CustomTaskSensor, self).poke(context)