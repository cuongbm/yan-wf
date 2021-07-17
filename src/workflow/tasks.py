import sys
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, List
from enum import Enum


from workflow.initializer import get_task_cls
from workflow._utils import guard_not_null


class BaseTask:
    def __init__(self, **kwargs):
        for field_name in kwargs:
            setattr(self, field_name, kwargs[field_name])


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

        self.tasks: Dict[str, BaseTask] = {}
        if definitions:
            for task_name in definitions["tasks"]:
                task_definition = definitions["tasks"][task_name]
                task_cls = get_task_cls(
                    definitions["modules"],
                    task_name)

                task_instance = task_cls(
                    **task_definition["parameters"])
                self.tasks[task_name] = task_instance

    def get_task(self, name):
        guard_not_null(name)
        return self.tasks.get(name,  None)

    def run(self):
        for task_name in self.tasks:
            self.tasks[task_name].run()
        self.status = RunStatus.SUCCESS


class RunStatus(Enum):
    NOT_STARTED = 1
    SUCCESS = 0
    ERROR = 2
