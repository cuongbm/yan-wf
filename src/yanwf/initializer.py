from importlib import import_module
from typing import Dict, List


def get_task_cls(module_names: List[str], task_cls: Dict):

    try:
        if "." in task_cls:
            module_path, class_name = task_cls.rsplit('.', 1)
            module = import_module(module_path)
            return getattr(module, class_name)
        else:
            for module_name in module_names:
                module = try_import_module(module_name)
                if module:
                    return getattr(module, task_cls)
            raise ValueError(f"Cannot get tasks class: {task_cls}. "
                             f"Looks up in {module_names}")
    except (ImportError, AttributeError) as e:
        raise ImportError(task_cls) from e


def try_import_module(name):
    try:
        return import_module(name)
    except ImportError:
        return None
