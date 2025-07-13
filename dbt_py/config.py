"""
Configuration for the package.
"""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import tomllib

import dbt_py.exceptions

CONFIG_FILE = "pyproject.toml"
TOOL_SECTION = ("tool", "dbt-py")
DEFAULT_PACKAGE_NAME = "custom"


@dataclasses.dataclass(frozen=True)
class Config:
    """
    Configuration for the package.
    """

    packages: list[dict[str, str]] = dataclasses.field(default_factory=list)

    @classmethod
    def from_dict(cls, settings: dict) -> Config:
        """
        Create a Config instance from a dictionary.
        """

        return cls(**settings)

    @property
    def hashable_packages(self):
        """
        Return a frozenset of JSON strings representing the packages.

        This is required for playing nicely with dbt's context modules.
        """
        return frozenset(json.dumps(pkg) for pkg in self.packages)


def _default_packages() -> dict[str, list[dict]]:
    """
    Return the default packages for the configuration.
    """

    # Python-style ref, e.g. `package.module.submodule`
    package_root = os.environ.get("DBT_PY_PACKAGE_ROOT")
    package_name = os.environ.get("DBT_PY_PACKAGE_NAME")
    if package_root or package_name:
        dbt_py.exceptions.warn(
            "Using environment variables for package configuration is deprecated. "
            "Use the `pyproject.toml` file instead.",
            dbt_py.exceptions.DbtPyWarning,
        )

    root = package_root or DEFAULT_PACKAGE_NAME
    name = package_name or package_root

    return {
        "packages": [
            {
                "name": name,
                "path": root,
            }
        ]
    }


def load_config(config_root: pathlib.Path) -> Config:
    """
    Load the configuration from the configuration file.
    """

    conf = config_root / CONFIG_FILE
    default = _default_packages()

    if not conf.exists():
        return Config.from_dict(default)

    return Config.from_dict(
        tomllib.loads(conf.read_text())
        .get(TOOL_SECTION[0], {})
        .get(TOOL_SECTION[1], default)
    )
