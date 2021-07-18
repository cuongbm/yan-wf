from workflow.tasks import BaseTask, Number, Parameter, String
from workflow.workflows import RunStatus, Workflow
from workflow.exceptions import PauseWorkflowException
from os import path
import pytest
import pickle


class MyWorkflow(Workflow):
    def __init__(self, **kwargs):
        super(MyWorkflow, self).__init__(**kwargs)


class ReadFileTask(BaseTask):
    path = String()

    def init(self):
        self.content = None

    def run(self):
        with open(self.path) as f:
            self.content = f.read()

    def output(self):
        return {"content": self.content}


class RunLimit(BaseTask):
    count = Number(default_value=0)
    limit = Number(default_value=1)

    def run(self):
        self.count += 1
        if self.count > self.limit:
            raise InterruptedError(
                f"Run count {self.count}  > limit {self.limit}")


class VerifyConnectTask(BaseTask):
    content = String()
    verify_content = String()
    required_content = String(required=True)

    def run(self):
        print(self.content)
        print(self.required_content)
        assert self.content == self.verify_content


class HibernateTask(BaseTask):
    time_in_ms = Number(default_value=1000)
    run_count = Number(default_value=1)

    def init(self):
        pass

    def run(self):
        if self.run_count == 1:
            self.run_count += 1
            raise PauseWorkflowException("test pause workflow")


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

    def test_resume_workflow(self):
        workflow_instance = MyWorkflow(
            raise_on_error=True,
            definitions={
                "modules": ["tests.test_workflow.test_workflow"],
                "context": {
                    "backend_profile": "sql::conn1",
                    "test_output": ""
                },
                "tasks": {
                    "RunLimit": {
                        "parameters": {
                            "limit": 1
                        }
                    },
                    "ReadFileTask": {
                        "parameters": {
                            "path": path.join(path.dirname(path.realpath(__file__)), "test.txt")
                        }
                    },
                    "HibernateTask": {
                        "name": "HibernateTask",
                        "parameters": {
                        }
                    }
                }
            })

        workflow_instance.run()

        assert workflow_instance.status == RunStatus.PAUSED
        assert workflow_instance.task_run_stats["ReadFileTask"].status == RunStatus.SUCCESS
        assert workflow_instance.task_run_stats["ReadFileTask"].start_time
        assert workflow_instance.task_run_stats["ReadFileTask"].end_time
        assert workflow_instance.task_run_stats["ReadFileTask"].output
        assert workflow_instance.task_run_stats["HibernateTask"].status == RunStatus.PAUSED

        workflow_instance.resume()

        assert workflow_instance.task_run_stats["HibernateTask"].status == RunStatus.SUCCESS
        assert workflow_instance.status == RunStatus.SUCCESS

    def test_save_state_then_resume(self):
        workflow_instance = MyWorkflow(
            raise_on_error=True,
            definitions={
                "modules": ["tests.test_workflow.test_workflow"],
                "context": {
                    "backend_profile": "sql::conn1",
                    "test_output": ""
                },
                "tasks": {
                    "RunLimit": {
                        "parameters": {
                            "limit": 1
                        }
                    },
                    "ReadFileTask": {
                        "parameters": {
                            "path": path.join(path.dirname(path.realpath(__file__)), "test.txt")
                        }
                    },
                    "HibernateTask": {
                        "name": "HibernateTask",
                        "parameters": {
                        }
                    }
                }
            })

        workflow_instance.run()

        # state = workflow_instance.get_state()
        # state = pickle.loads(pickle.dumps(state))
