from celery import Celery
from urllib.parse import quote
from time import sleep

from yanwf.workflows import Workflow
from yanwf.tasks import Number, String, BaseTask
from yanwf.exceptions import PauseWorkflowException

app = Celery(
    "tasks",
    broker="amqp://172.18.106.117:31672",
    backend=f'db+mysql://root:{quote("ventum2011@")}@localhost/world',
)


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


@app.task
def reverse(text):
    sleep(5)
    return text[::-1]


@app.task
def test_run_wf_on_celery2(definitions):
    workflow_instance = MyWorkflow(definitions=definitions)
    # workflow_instance = MyWorkflow(
    #     definitions={
    #         "modules": [__name__, "run_celery.start_celery"],
    #         "tasks": {"HibernateTask": {"name": "HibernateTask", "parameters": {"time_in_ms": 1000}}},
    #     }
    # )
    # workflow_instance.run()
    workflow_instance.run()
    return workflow_instance.status
