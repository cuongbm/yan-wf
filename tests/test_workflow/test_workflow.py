from workflow.tasks import BaseTask, Number, Parameter, String
from workflow.workflows import RunStatus, Workflow
from os import path
import pytest


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


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
    required_content = String(required=True)

    def run(self):
        print(self.content)
        print(self.required_content)
        assert self.content == self.verify_content


class TestWorkflowRun:
    def test_run_workflow_error(self):
        workflow_instance = MyWorkflow(definitions={
            "modules": ["tests.test_workflow.test_tasks", "tests.test_workflow.test_workflow"],
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
                "modules": ["tests.test_workflow.test_workflow"],
                "tasks": {
                    "ReadFileTask": {
                        "parameters": {
                            "path": path.join(path.dirname(path.realpath(__file__)), "test.txt")
                        }
                    },
                    "VerifyConnectTask": {
                        "name": "VerifyConnectTask",
                        "parameters": {
                            "content": "$context.content",
                            "verify_content": "this is a sample content",
                            "required_content": "any"
                        }
                    }
                }
            })

        workflow_instance.run()

    def test_connect_required_parameters(self):
        workflow_instance = MyWorkflow(
            raise_on_error=True,
            definitions={
                "modules": ["tests.test_workflow.test_workflow"],
                "context": {
                    "backend_profile": "sql::conn1",
                    "test_output": ""
                },
                "tasks": {
                    "ReadFileTask": {
                        "parameters": {
                            "path": path.join(path.dirname(path.realpath(__file__)), "test.txt")
                        }
                    },
                    "VerifyConnectTask": {
                        "name": "VerifyConnectTask",
                        "parameters": {
                            "required_content": "$context.test_output",
                        }
                    }
                }
            })

        with pytest.raises(ValueError):
            workflow_instance.run()
