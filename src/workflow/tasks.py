from collections.abc import MutableMapping
from workflow.utils import guard_not_null
from workflow.initializer import get_task_cls
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, List
from enum import Enum
import logging
import traceback

logger = logging.getLogger(__name__)


class WorkflowContext(MutableMapping):
    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        if not "." in key:
            return self.store[self._keytransform(key)]

        current_value = self.store
        for part in key.split("."):
            current_value = current_value[part]
        return current_value

    def __setitem__(self, key, value):
        self.store[self._keytransform(key)] = value

    def __delitem__(self, key):
        del self.store[self._keytransform(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def _keytransform(self, key):
        return key


class BaseTask(ABC):
    def __init__(self, name: str = None, workflow_context: WorkflowContext = None, **kwargs):
        if name:
            self.name = name
        for field_name in kwargs:
            setattr(self, field_name, kwargs[field_name])

    @abstractmethod
    def run(self):
        pass

    def output(self):
        pass

    def __repr__(self):
        return getattr(self, "name", super().__repr__())


class Parameter(ABC):

    def __init__(self, default_value=None):
        self.validate(default_value)
        self.default_value = default_value

    def __set_name__(self, owner, name):
        self.private_name = '_' + name

    def __get__(self, obj, objtype=None):
        result = self.default_value
        if hasattr(obj, self.private_name):
            result = getattr(obj, self.private_name)

        if isinstance(result, str) and result.startswith("$context."):
            key = result[9:]
            return self._get_from_context(key, obj)

        return result

    def _get_from_context(self, key, obj):
        if not hasattr(obj, "workflow_context"):
            raise ValueError("workflow_context not found")
        if not obj.workflow_context:
            raise ValueError(
                "workflow_context is not set. Cannot access with $context")
        return obj.workflow_context[key]

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
