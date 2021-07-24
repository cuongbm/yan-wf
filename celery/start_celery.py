from celery import Celery
from time import sleep
from urllib.parse import quote

from workflow.workflows import Workflow


app = Celery('tasks',
             broker='amqp://172.18.106.117:31672',
             backend=f'db+mysql://root:{quote("ventum2011@")}@localhost/world')


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


workflow_instance = MyWorkflow(definitions={
    "modules": [
        "tests.test_workflow.test_tasks",
        "tests.test_workflow.test_workflow"],
    "tasks": {
        "CalledTask": {
            "parameters": {}
        },
        "RunErrorTask": {
            "name": "run_error_task",
            "parameters": {
                "error_message": "this is a test error"
            }
        }
    }
})

workflow_instance.run()


@app.task
def reverse(text):
    sleep(5)
    return text[::-1]


def test_run_wf_on_celery():

    result = reverse.delay('123')
    print(result.get(timeout=10))
