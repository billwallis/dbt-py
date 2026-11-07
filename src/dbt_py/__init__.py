"""
Python wrapper for dbt-core to extend dbt with custom Python.
"""

from dbt_py.main import PROJECT_ROOT, main

__all__ = [
    "PROJECT_ROOT",
    "main",
]
