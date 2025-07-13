"""
Shim the dbt CLI to include our custom modules.
"""

import functools
import importlib
import json
import pathlib
import pkgutil
import sys
from types import ModuleType
from typing import Any

import dbt.cli.main
import dbt.context.base
from dbt.context.base import get_context_modules as _get_context_modules

import dbt_py.config
import dbt_py.exceptions

PROJECT_ROOT = pathlib.Path(__file__).parent.parent

JSON = str


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
        return {}  # pragma: no cover

    results = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = f"{package.__name__}.{name}"
        results[full_name] = importlib.import_module(full_name)
        if recursive and is_pkg:
            results |= _import_submodules(full_name)  # pragma: no cover

    return results


@functools.cache
def _get_context_modules_shim(
    packages: list[JSON],
) -> dict[str, dict[str, Any]]:
    """
    Append the custom modules into the whitelisted dbt modules.
    """

    modules = _get_context_modules()
    # for hashability, the packages are stored as JSON strings
    for package in packages:
        pkg = json.loads(package)
        name = pkg["name"]
        path = pkg.get("path", name)
        try:
            _import_submodules(path)
            modules[name] = importlib.import_module(path)  # type: ignore
        except ModuleNotFoundError as err:
            # This should probably only be masked when the default custom
            # modules are being imported. Otherwise, we're unable to import
            # a module that is expected to be present, which is a problem.

            # warnings.warn(str(err), dbt_py.exceptions.DbtPyWarning)
            dbt_py.exceptions.warn(
                f"failed to import package '{path}': {err}",
                dbt_py.exceptions.DbtPyWarning,
            )

    return modules


def main(config_root: str = ".") -> None:
    """
    Shim the dbt CLI to include our custom modules.

    - https://docs.getdbt.com/reference/programmatic-invocations
    """

    conf = dbt_py.config.load_config(pathlib.Path(config_root))
    shim = functools.partial(
        _get_context_modules_shim,
        packages=conf.hashable_packages,
    )

    dbt.context.base.get_context_modules = shim
    result = dbt.cli.main.dbtRunner().invoke(sys.argv[1:])

    if result.success:
        raise SystemExit(0)
    if result.exception is None:
        raise SystemExit(1)
    raise SystemExit(2)
