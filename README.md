<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.5+-blue.svg)](https://github.com/dbt-labs/dbt-core/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![tests](https://github.com/billwallis/dbt-py/actions/workflows/tests.yaml/badge.svg)](https://github.com/billwallis/dbt-py/actions/workflows/tests.yaml)
[![coverage](coverage.svg)](https://github.com/dbrgn/coverage-badge)

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/billwallis/dbt-py/main.svg)](https://results.pre-commit.ci/latest/github/billwallis/dbt-py/main)
[![GitHub last commit](https://img.shields.io/github/last-commit/billwallis/dbt-py)](https://shields.io/badges/git-hub-last-commit)
[![PyPI downloads](https://img.shields.io/pypi/dm/dbt-py-wrap)](https://shields.io/badges/py-pi-downloads)

</div>

---

# dbt-π 🧬

Python wrapper for [dbt-core](https://github.com/dbt-labs/dbt-core) to extend dbt with custom Python.

## Shimmy shimmy shim 🕺🕺🕺

This package is a [shim](<https://en.wikipedia.org/wiki/Shim_(computing)>) for [dbt-core](https://github.com/dbt-labs/dbt-core), inspired by (_cough_ stolen from _cough_) my old boss, [@darkdreamingdan](https://github.com/darkdreamingdan):

- https://gist.github.com/darkdreamingdan/c5ded709a90fc3c5b420cee5f644f499

Before using this package, it's recommended to get up to speed with the Python modules that are already available in dbt:

- https://docs.getdbt.com/reference/dbt-jinja-functions/modules

The existing Python modules are available in the dbt Jinja context under the `modules` object, for example:

```jinja
{{ modules.datetime.datetime.now() }}
```

## Installation ⬇️

Grab a copy from PyPI like usual:

```
pip install dbt-py-wrap
```

Note the `-wrap` suffix, which is used to avoid name clashes with [the `DbtPy` PyPI package](https://pypi.org/project/DbtPy/).

## Usage 📖

> [!IMPORTANT]
>
> If you create the Python files in your dbt repo, you **must**:
>
> - make your custom modules/packages discoverable by Python
> - install your own project

> [!TIP]
>
> See the following repo for a minimal example that uses `requirements.txt` and [setuptools](https://setuptools.pypa.io/en/latest/):
>
> - [https://github.com/billwallis/dbt-py-test](https://github.com/billwallis/dbt-py-test)

This package adds a new executable, `dbt-py`, which injects your custom Python into dbt and then runs dbt. Custom modules, custom packages, standard library packages, and installed packages can be injected.

### Configuration (>=0.1.0)

From version `0.1.0` onwards, the packages to inject are configured via the `pyproject.toml` file in the `tool.dbt-py` section. Use the `packages` attribute to list the `name` (required) and `path` (optional) of the modules to import; for example:

```toml
[tool.dbt-py]
packages = [
    {name = "math"},  # stdlib package
    {name = "dbt_py"},  # installed package
    {name = "custom_module"},  # custom module
    {name = "custom_package"},  # custom package
    {name = "another_package", path = "another_custom_package"},  # custom package with path
]
```

The custom modules/packages can only be imported by Python/dbt if they are discoverable by Python. For example, if you're using [setuptools](https://setuptools.pypa.io/en/latest/index.html) and configuring it with the `pyproject.toml` file, this is typically [achieved by specifying the `py-modules` and `packages` attributes](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html):

```toml
[tool.setuptools]
py-modules = [
    "custom_module",
]
packages = [
    "custom_package",
    "another_custom_package",
]
```

You also need to install your project so that the custom modules/packages are available in your Python environment; for example:

```
pip install -e .
```

If you don't configure `dbt-py` in the `pyproject.toml` file, the configuration will default to the legacy behaviour (below).

### Configuration (<0.1.0)

> [!WARNING]
>
> This configuration is deprecated and will be removed in a future version of `dbt-py`. Use the `pyproject.toml` configuration instead.

Before version `0.1.0`, `dbt-py` only supported a single custom module/package. This was configured via the following environment variables:

- `DBT_PY_PACKAGE_ROOT`: The Python-style ref to the custom module/package, e.g. `package.module.submodule`
- `DBT_PY_PACKAGE_NAME`: The name to give the custom module/package in the dbt Jinja context, e.g. `custom_py`. Defaults to the value of `DBT_PY_PACKAGE_ROOT`

If not specified, the `DBT_PY_PACKAGE_ROOT` defaults to `custom`.


### Custom Module 🐍

Create a module called `custom.py` in the root of your dbt project. This module can contain any Python code you like, for example:

```python
def salutation(name: str) -> str:
    return f"Hello, {name}!"
```

Add this module to the `pyproject.toml` file for your build system and for `dbt-py`:

```toml
[tool.setuptools]
py-modules = [
    "custom",
]

[tool.dbt-py]
packages = [
    {name = "custom"},
]
```

Install your project:

```
pip install -e .
```

Reference this module and function in the dbt Jinja context of a dbt model:

```jinja
{{ modules.custom.salutation("World") }}
```

Rather than run dbt with the `dbt` command, instead run it with `dbt-py`:

```
dbt-py clean
dbt-py build
```

Note that `dbt-py` is a wrapper around `dbt` so all the usual dbt commands are available -- all the arguments passed to `dbt-py` are passed through to `dbt`, too.

```
dbt-py --help
dbt-py run --select my_model
dbt-py test --select tag:unit-test
```

### Custom Package 📦

Using a custom package is similar to using a custom module: create a package called `custom` in the root of your dbt project.

The submodules of this package will be available in the dbt Jinja context too. For example, suppose you have a package called `custom` with a submodule called `greetings`:

```
custom/
    __init__.py
    greetings.py
```

Add this package to the `pyproject.toml` file for your build system and for `dbt-py`:

```toml
[tool.setuptools]
packages = [
    "custom",
]

[tool.dbt-py]
packages = [
    {name = "custom"},
]
```

Install your project:

```
pip install -e .
```

If the `greetings.py` submodule contains the same `salutation` function as above, then it can be referenced in the dbt Jinja context as follows:

```jinja
{{ modules.custom.greetings.salutation("World") }}
```

Alternatively, you can expose the `salutation` function via the `__init__.py` file and then reference it directly via `custom`:

```jinja
{{ modules.custom.salutation("World") }}
```

## Future Work 🚧

This is still in preview, and the API is likely to change considerably over time.

For example:

- the environment variable configuration will be deprecated in favour of the `pyproject.toml` configuration
- the `pyproject.toml` configuration may piggyback off of the build system (e.g. setuptools) configuration, rather than being a separate section

## Contributing 🤝

Raise an issue, or fork the repo and open a pull request.

This project uses [uv](https://github.com/astral-sh/uv/) and [pre-commit](https://pre-commit.com/). After cloning the repo, install the dependencies and enable pre-commit:

```
uv sync --all-groups
pre-commit install --install-hooks
```
