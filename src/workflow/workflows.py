
import sys
import logging
import traceback
from datetime import datetime, timedelta

from abc import ABC, abstractmethod
from importlib import import_module
from typing import Any, Dict, List
from enum import Enum

from workflow.exceptions import PauseWorkflowException, TaskRunningException
from workflow.utils import guard_not_null
from workflow.initializer import get_task_cls
from workflow.tasks import BaseTask, WorkflowContext
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
    context = attr.ib(type=WorkflowContext, default=WorkflowContext)
    task_run_stats = attr.ib(
        type=Dict[str, TaskRunStat], default=dict)


class Workflow:
    def __init__(self, raise_on_error=False, definitions=None):
        self.raise_on_error = raise_on_error
        
        self.tasks: Dict[str, BaseTask] = {}

        self.state = WorkflowState()

        self.status = RunStatus.NOT_STARTED
        self.trace = None
        self.error = None
        self.context = WorkflowContext()
        
        self.task_run_stats: Dict[str, TaskRunStat] = {}

        if definitions:
            self.context.update(definitions.get("context", {}))
            self._create_tasks(definitions)

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
        return self.tasks.get(task_name,  None)

    def run(self):
        try:
            for task_name in self.tasks:
                self._run_task(task_name)

            self.status = RunStatus.SUCCESS

        except PauseWorkflowException as e:
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
