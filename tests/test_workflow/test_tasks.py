from workflow.tasks import BaseTask, Number, Parameter, RunStatus, String, Workflow
from os import path


class MyTask(BaseTask):
    param1 = String()
    param2 = String()
    param3 = Number(default_value=154)

    def run(self):
        pass


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


class CalledTask(BaseTask):
    def __init__(self, **kwargs):
        self.run_count = 0
        super(CalledTask, self).__init__(**kwargs)

    def run(self):
        self.run_count += 1


class TestWorkflowDefinition():
    def test_define_then_set_params(self):
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

    def test_define_workflow(self):
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

    def test_run_workflow(self):
        workflow_instance = MyWorkflow(definitions={
            "modules": ["tests.test_workflow.test_tasks"],
            "tasks": {
                "CalledTask": {
                    "parameters": {}
                },
                "CalledTask2": {
                    "cls": "CalledTask",
                    "parameters": {}
                }
            }
        })

        assert workflow_instance.status == RunStatus.NOT_STARTED

        workflow_instance.run()

        assert workflow_instance.get_task("CalledTask").run_count
        assert workflow_instance.get_task("CalledTask2").run_count
        assert workflow_instance.status == RunStatus.SUCCESS


class RunErrorTask(BaseTask):
    error_message = String(default_value="wth")

    def run(self):
        raise ValueError(self.error_message)


class ReadFileTask(BaseTask):
    path = String()

    def __init__(self, **kwargs):
        self.content = None
        super().__init__(**kwargs)

    def run(self):
        with open(self.path) as f:
            self.content = f.read()

    def output(self):
        return {"content": self.content}


class VerifyConnectTask(BaseTask):
    content = String()
    verify_content = String()

    def run(self):
        print(self.content)
        assert self.content == self.verify_content


class TestWorkflowRun:
    def test_run_workflow_error(self):
        workflow_instance = MyWorkflow(definitions={
            "modules": ["tests.test_workflow.test_tasks"],
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

        assert workflow_instance.status == RunStatus.ERROR
        print(workflow_instance.trace)
        assert workflow_instance.trace
        assert workflow_instance.error

    def test_connect_parameters(self):
        workflow_instance = MyWorkflow(
            raise_on_error=True,
            definitions={
                "modules": ["tests.test_workflow.test_tasks"],
                "tasks": {
                    "ReadFileTask": {
                        "parameters": {
                            "path": path.join(path.dirname(path.realpath(__file__)), "test.txt")
                        }
                    },
                    "VerifyConnectTask": {
                        "name": "VerifyConnectTask",
                        "parameters": {
                            "content": "this is a sample content",
                            "verify_content": "this is a sample content"
                        }
                    }
                }
            })

        workflow_instance.run()
