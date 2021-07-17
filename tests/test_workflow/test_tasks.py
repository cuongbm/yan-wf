from workflow.tasks import BaseTask, Number, Parameter, String, WorkflowContext
from workflow.workflows import RunStatus, Workflow
from os import path
import pytest
import pickle


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


class TestWorkflowContext:
    def test_change_context(self):
        context = WorkflowContext()
        context["a"] = "b"
        assert context["a"] == context.get("a") == "b"

        context["key1"] = {
            "key2": {
                "key3": "value4"
            }
        }

        assert context["key1.key2"] == {
            "key3": "value4"
        }

        assert context["key1.key2.key3"] == "value4"

        # key doest not exist
        with pytest.raises(KeyError):
            _ = context["key1.key2.key5"]

    def test_pickle(self):
        context = WorkflowContext({
            "a": "b",
            "c": {
                "d": 1
            }
        })
        assert pickle.loads(pickle.dumps(context)) == context
