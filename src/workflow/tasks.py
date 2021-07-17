from workflow._utils import guard_not_null
from workflow.initializer import get_task_cls
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, List
from enum import Enum
import logging
import traceback

logger = logging.getLogger(__name__)


class BaseTask:
    def __init__(self, name: str = None, **kwargs):
        if name:
            self.name = name
        for field_name in kwargs:
            setattr(self, field_name, kwargs[field_name])

    def __repr__(self):
        return getattr(self, "name", super().__repr__())


class Parameter(ABC):

    def __init__(self, default_value=None):
        self.validate(default_value)
        self.default_value = default_value

    def __set_name__(self, owner, name):
        self.private_name = '_' + name

    def __get__(self, obj, objtype=None):
        if hasattr(obj, self.private_name):
            return getattr(obj, self.private_name)
        return self.default_value

    def __set__(self, obj, value):
        self.validate(value)
        logger.debug(f"Set value of {obj} to {value}")
        setattr(obj, self.private_name, value)

    @abstractmethod
    def validate(self, value):
        print(f'validate {value}')


class String(Parameter):
    def validate(self, value):
        pass


class Number(Parameter):
    def validate(self, value):
        int(value)


class Workflow:
    def __init__(self, definitions=None):
        self.status = RunStatus.NOT_STARTED
        self.trace = None
        self.error = None

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

    def get_task(self, name):
        guard_not_null(name)
        return self.tasks.get(name,  None)

    def run(self):
        try:
            for task_name in self.tasks:
                self.tasks[task_name].run()
            self.status = RunStatus.SUCCESS
        except Exception as e:
            self.trace = traceback.format_exc()
            self.status = RunStatus.ERROR
            self.error = e


class RunStatus(Enum):
    NOT_STARTED = 1
    SUCCESS = 0
    ERROR = 2
