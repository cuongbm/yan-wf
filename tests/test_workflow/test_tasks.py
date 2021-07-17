from workflow.tasks import BaseTask, Number, Parameter, RunStatus, String, Workflow


class MyTask(BaseTask):
    param1 = String()
    param2 = String()
    param3 = Number(default_value=154)


def test_define_then_set_params():
    # set via constructor
    task1 = MyTask(param1="abc")
    assert task1.param1 == "abc"

    # no value is defined
    assert not task1.param2

    # default value
    assert task1.param3 == 154

    task1 = MyTask(param1="def", param3=451)

    # override the default value
    assert task1.param3 == 451


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


def test_define_workflow():
    workflow_instance = MyWorkflow(definitions={
        "modules": ["tests.test_workflow.test_tasks"],
        "tasks": {
            "MyTask": {
                "parameters": {
                    "param1": "abc",
                    "param2": "def",
                    "param4": "${}"
                }
            }
        }
    })

    assert workflow_instance.get_task("MyTask")


class CalledTask(BaseTask):
    def __init__(self, **kwargs):
        self.run_count = 0
        super(CalledTask, self).__init__(**kwargs)

    def run(self):
        self.run_count += 1


def test_run_workflow():
    workflow_instance = MyWorkflow(definitions={
        "modules": ["tests.test_workflow.test_tasks"],
        "tasks": {
            "CalledTask": {
                "parameters": {}
            }
        }
    })

    assert workflow_instance.status == RunStatus.NOT_STARTED

    workflow_instance.run()

    assert workflow_instance.get_task("CalledTask").run_count

    assert workflow_instance.status == RunStatus.SUCCESS
