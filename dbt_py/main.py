"""
Shim the dbt CLI to include our custom modules.
"""

import importlib
import os
import pathlib
import pkgutil
import sys
from types import ModuleType
from typing import Any

import dbt.cli.main
import dbt.context.base
from dbt.context.base import get_context_modules as _get_context_modules

PROJECT_ROOT = pathlib.Path(__file__).parent.parent


def _import_submodules(
    package_name: str,
    recursive: bool = True,
) -> dict[str, ModuleType]:
    """
    Import all submodules of a module, recursively, including subpackages.

    - https://stackoverflow.com/a/25562415/10730311
    """

    package = importlib.import_module(package_name)
    if not hasattr(package, "__path__"):
        # `package` is a module, don't recurse any further
        return {}

    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = f"{package.__name__}.{name}"
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results |= _import_submodules(full_name)

    return results


def _get_context_modules_shim() -> dict[str, dict[str, Any]]:
    """
    Append the custom modules into the whitelisted dbt modules.
    """

    # Python-style ref, e.g. `package.module.submodule`
    package_root: str = os.environ.get("DBT_PY_PACKAGE_ROOT") or "custom"
    package_name: str = os.environ.get("DBT_PY_PACKAGE_NAME") or package_root

    _import_submodules(package_root)
    modules = _get_context_modules()
    modules[package_name] = importlib.import_module(package_root)  # type: ignore

    return modules


def main() -> None:
    """
    Shim the dbt CLI to include our custom modules.

    - https://docs.getdbt.com/reference/programmatic-invocations
    """

    dbt.context.base.get_context_modules = _get_context_modules_shim
    result = dbt.cli.main.dbtRunner().invoke(sys.argv[1:])

    if result.success:
        raise SystemExit(0)
    if result.exception is None:
        raise SystemExit(1)
    raise SystemExit(2)
