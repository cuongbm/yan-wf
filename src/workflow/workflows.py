from workflow.utils import guard_not_null
from workflow.initializer import get_task_cls
from workflow.tasks import BaseTask
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, List
from enum import Enum
import logging
import traceback


class Workflow:
    def __init__(self, raise_on_error=False, definitions=None):
        self.raise_on_error = raise_on_error
        self.status = RunStatus.NOT_STARTED
        self.trace = None
        self.error = None
        self.context = {}

        self.tasks: Dict[str, BaseTask] = {}
        if definitions:
            for task_name in definitions["tasks"]:
                task_definition = definitions["tasks"][task_name]
                cls_name = task_definition.get("cls", task_name)
                task_cls = get_task_cls(
                    definitions["modules"],
                    cls_name)

                task_instance = task_cls(
                    name=task_definition.get("name", task_name),
                    **task_definition["parameters"])
                self.tasks[task_name] = task_instance

    def get_task(self, name) -> BaseTask:
        guard_not_null(name)
        return self.tasks.get(name,  None)

    def run(self):
        try:
            for task_name in self.tasks:
                self.tasks[task_name].run()
                output = self.tasks[task_name].output()
                if output:
                    if isinstance(output, dict):
                        self.context.update(output)
                    else:
                        self.context[task_name] = output

            self.status = RunStatus.SUCCESS
        except Exception as e:
            self.trace = traceback.format_exc()
            self.status = RunStatus.ERROR
            self.error = e
            if self.raise_on_error:
                raise


class WorkflowContext:
    def __init__(self, dict_: dict):
        self._dict = dict_


class RunStatus(Enum):
    NOT_STARTED = 1
    SUCCESS = 0
    ERROR = 2
