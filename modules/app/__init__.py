# modules/app/__init__.py
from .evaluator import _evaluate as evaluate, Dependencies, DataPaths

__all__ = ["evaluate", "Dependencies", "DataPaths"]
