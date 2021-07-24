import traceback
from datetime import datetime

from typing import Any, Dict
from enum import Enum

from yanwf.exceptions import PauseWorkflowException, TaskRunningException
from yanwf.utils import guard_not_null
from yanwf.initializer import get_task_cls
from yanwf.tasks import BaseTask, WorkflowContext
import attr


class RunStatus(Enum):
    NOT_STARTED = 1
    SUCCESS = 0
    ERROR = 3
    PAUSED = 4,
    RUNNING = 5


@attr.s
class TaskRunStat:
    start_time = attr.ib(type=datetime, default=datetime.utcnow)
    end_time = attr.ib(type=datetime, default=None)
    status = attr.ib(type=RunStatus, default=RunStatus.NOT_STARTED)
    status_text = attr.ib(type=str, default=None)
    output = attr.ib(type=Any, default=None)


@attr.s
class WorkflowState:
    status = attr.ib(type=RunStatus, default=RunStatus.NOT_STARTED)
    trace = attr.ib(type=Any, default=None)
    error = attr.ib(type=Any, default=None)
    context = attr.ib(type=WorkflowContext, factory=WorkflowContext)
    task_run_stats = attr.ib(
        type=Dict[str, TaskRunStat], factory=dict)


class Workflow:
    def __init__(self, raise_on_error=False, definitions=None):
        self.raise_on_error = raise_on_error

        self.state = WorkflowState()

        self.tasks: Dict[str, BaseTask] = {}

        if definitions:
            self.context.update(definitions.get("context", {}))
            self._create_tasks(definitions)

    @property
    def status(self):
        return self.state.status

    @status.setter
    def status(self, value):
        self.state.status = value

    @property
    def trace(self):
        return self.state.trace

    @trace.setter
    def trace(self, value):
        self.state.trace = value

    @property
    def error(self):
        return self.state.error

    @error.setter
    def error(self, value):
        self.state.error = value

    @property
    def context(self) -> WorkflowContext:
        return self.state.context

    @property
    def task_run_stats(self) -> Dict[str, TaskRunStat]:
        return self.state.task_run_stats

    def _create_tasks(self, definitions):
        for task_name in definitions["tasks"]:
            task_definition = definitions["tasks"][task_name]
            cls_name = task_definition.get("cls", task_name)

            task_cls = get_task_cls(
                module_names=definitions["modules"],
                task_cls=cls_name)

            task_instance = task_cls(
                name=task_definition.get("name", task_name),
                **task_definition["parameters"])

            task_instance.workflow_context = self.context
            self.tasks[task_name] = task_instance

    def get_task(self, task_name) -> BaseTask:
        guard_not_null(task_name)
        return self.tasks.get(task_name, None)

    def run(self):
        try:
            for task_name in self.tasks:
                self._run_task(task_name)

            self.status = RunStatus.SUCCESS

        except PauseWorkflowException:
            self.status = RunStatus.PAUSED
        except Exception as e:
            self.trace = traceback.format_exc()
            self.status = RunStatus.ERROR
            self.error = e
            if self.raise_on_error:
                raise

    def _run_task(self, task_name):
        if task_name in self.task_run_stats:
            # skip in case of resume
            if self.task_run_stats[task_name].status == RunStatus.SUCCESS:
                return

            if self.task_run_stats[task_name].status == RunStatus.RUNNING:
                raise TaskRunningException(f"{task_name} is running")

        try:
            if task_name not in self.task_run_stats:
                self.task_run_stats[task_name] = TaskRunStat()
            self.tasks[task_name].run()

            # process output
            output = self.tasks[task_name].output()
            if output:
                if isinstance(output, dict):
                    self.context.update(output)
                else:
                    self.context[task_name] = output

            # update stat
            self.task_run_stats[task_name].status = RunStatus.SUCCESS
            self.task_run_stats[task_name].end_time = datetime.utcnow()
            self.task_run_stats[task_name].output = output
        except PauseWorkflowException as e:
            self.task_run_stats[task_name].status = RunStatus.PAUSED
            self.task_run_stats[task_name].status_text = str(e)
            self.task_run_stats[task_name].end_time = datetime.utcnow()

            raise

    def resume(self):
        self.run()
