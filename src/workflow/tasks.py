from collections.abc import MutableMapping
from workflow.utils import guard_not_null
from workflow.initializer import get_task_cls
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from typing import Dict, List, Any
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
    def __init__(self, name: str = None, **kwargs):
        if name:
            self.name = name
        self.workflow_context = None
        for field_name in kwargs:
            setattr(self, field_name, kwargs[field_name])

        self.init()

    @property
    def workflow_context(self):
        return self._workflow_context

    @workflow_context.setter
    def workflow_context(self, value):
        self._workflow_context = value

    def init(self):
        pass

    @abstractmethod
    def run(self):
        pass

    def output(self) -> Any:
        """Return value of output will be updated to workflow context

        Returns:
            Any:   
             - None: No update
             - Dict: Merge will workflow context
             - Other: context[task_name] = output
        """
        pass

    def __repr__(self):
        return getattr(self, "name", super().__repr__())


class Parameter(ABC):

    def __init__(self, default_value=None, required=False):
        self.validate(default_value)
        self.default_value = default_value
        self.required = required

    def __set_name__(self, owner, name):
        self.private_name = '_' + name
        self.original_name = name

    def __get__(self, obj, objtype=None):
        result = self.default_value
        if hasattr(obj, self.private_name):
            result = getattr(obj, self.private_name)

        if isinstance(result, str) and result.startswith("$context."):
            key = result[9:]
            result = self._get_from_context(key, obj)

        self._validate_required(result)

        return result

    def _get_from_context(self, key, obj):
        if not hasattr(obj, "workflow_context"):
            raise ValueError("workflow_context not found")
        if not obj.workflow_context:
            raise ValueError(
                "workflow_context is not set. Cannot access with $context")
        return obj.workflow_context[key]

    def _validate_required(self, value):
        if self.required and not value:
            raise ValueError(f"{self.original_name} is required")

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
        if value:
            int(value)
