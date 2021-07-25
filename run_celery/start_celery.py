from time import sleep
from urllib.parse import quote

from run_celery.celery_app import app, reverse, test_run_wf_on_celery2
from yanwf.workflows import Workflow
from yanwf.tasks import Number, String, BaseTask
from yanwf.exceptions import PauseWorkflowException


class RunErrorTask(BaseTask):
    error_message = String(default_value="wth")

    def run(self):
        print("OK")
        raise ValueError(self.error_message)


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
            raise InterruptedError(f"Run count {self.count}  > limit {self.limit}")


class VerifyConnectTask(BaseTask):
    content = String()
    verify_content = String()
    required_content = String(required=True)

    def run(self):
        print(self.content)
        print(self.required_content)
        assert self.content == self.verify_content


class PrintTextTask(BaseTask):
    text = String()

    def run(self):
        print(self.text)

    def output(self):
        return {"text": self.text}


class HibernateTask(BaseTask):
    time_in_ms = Number(default_value=1000)
    run_count = Number(default_value=1)

    def init(self):
        pass

    def run(self):
        if self.run_count == 1:
            self.run_count += 1
            raise PauseWorkflowException("test pause workflow")


if __name__ == "__main__":
    result1 = test_run_wf_on_celery2.delay(
        definitions={
            "modules": [__name__, "run_celery.start_celery"],
            "tasks": {"HibernateTask": {"name": "HibernateTask", "parameters": {"time_in_ms": 1000}}},
        }
    )

    print("done")
