"""
Integration tests for the package.
"""

import contextlib
import dataclasses
import shutil
import textwrap
import unittest.mock
from collections.abc import Generator
from typing import Any

import dbt.cli.main
import pytest

import dbt_py
from dbt_py.main import _get_context_modules_shim

pytestmark = pytest.mark.integration

DBT_PROJECT_DIR = dbt_py.PROJECT_ROOT / "tests/integration/jaffle-shop"
ARGS = [
    "--project-dir",
    str(DBT_PROJECT_DIR),
    "--profiles-dir",
    str(DBT_PROJECT_DIR),
]
EXAMPLE_FILE = (
    DBT_PROJECT_DIR / "target/compiled/jaffle_shop/models/example.sql"
)
EXAMPLE_COMPILED = textwrap.dedent(
    """
    select * from final
    Hello, World!

    select * from final
    Hello, World!
    """
)


@pytest.fixture(scope="function", autouse=True)
def clear_cache():
    """
    Clear the various caches.
    """

    # Need to invalidate the cache to reload the modules with different
    # environment variables
    _get_context_modules_shim.cache_clear()


@pytest.fixture(scope="function")
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Mock the environment variables used by dbt_py.
    """

    monkeypatch.setenv(
        "DBT_PY_PACKAGE_ROOT",
        "tests.integration.jaffle-shop.dbt_py_test",
    )
    monkeypatch.setenv(
        "DBT_PY_PACKAGE_NAME",
        "custom_py",
    )


@pytest.fixture(autouse=True)
def teardown() -> Generator[None, Any, None]:
    """
    Remove the dbt target directory if it exists.
    """

    yield

    # TODO: I think this should be dynamic
    target = DBT_PROJECT_DIR / "target"
    if target.exists():
        with contextlib.suppress(PermissionError):
            # TODO: figure out why we're getting these locks
            shutil.rmtree(target)


def test__missing_custom_packages_are_handled_gracefully(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Missing custom packages are handled gracefully.
    """

    monkeypatch.setenv("DBT_PY_PACKAGE_ROOT", "")
    monkeypatch.setenv("DBT_PY_PACKAGE_NAME", "")

    with unittest.mock.patch("sys.argv", ["", "debug", *ARGS]):
        with pytest.raises(SystemExit):
            dbt_py.main()

    captured = capsys.readouterr()
    msg = "DbtPyWarning: failed to import package 'custom': No module named 'custom'"

    assert msg in captured.out


def test__dbt_can_be_successfully_invoked(mock_env) -> None:
    """
    dbt can be successfully invoked.
    """

    with unittest.mock.patch("sys.argv", ["", "compile", *ARGS]):
        with pytest.raises(SystemExit) as exit_info:
            dbt_py.main()

    assert exit_info.value.code == 0
    assert (
        EXAMPLE_FILE.read_text(encoding="utf-8").strip()
        == EXAMPLE_COMPILED.strip()
    )


@pytest.mark.parametrize(
    "success, exception, expected_exit_code",
    [
        (True, None, 0),
        (False, None, 1),
        (False, Exception("something bad"), 2),
    ],
)
def test__errors_return_the_correct_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    success: bool,
    exception: BaseException | None,
    expected_exit_code: int,
) -> None:
    """
    The correct exit code is returned, following the dbt docs:

    - https://docs.getdbt.com/reference/programmatic-invocations#dbtrunnerresult
    """

    @dataclasses.dataclass
    class MockRunnerResult:
        success: bool
        exception: BaseException | None

    class MockRunner:
        def invoke(self, args):
            return MockRunnerResult(success=success, exception=exception)

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", MockRunner)

    with unittest.mock.patch("sys.argv", ["", "compile", *ARGS]):
        with pytest.raises(SystemExit) as exit_info:
            dbt_py.main()

    assert exit_info.value.code == expected_exit_code


def test__dbt_can_use_pyproject_config(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    dbt can be successfully invoked using the pyproject.toml config.
    """

    monkeypatch.setenv("DBT_PY_PACKAGE_ROOT", "")
    monkeypatch.setenv("DBT_PY_PACKAGE_NAME", "")

    with unittest.mock.patch("sys.argv", ["", "debug", *ARGS]):
        with pytest.raises(SystemExit) as exit_info:
            dbt_py.main("tests/integration/jaffle-shop")

    captured = capsys.readouterr()
    msg = "DbtPyWarning: failed to import package"

    assert exit_info.value.code == 0
    assert msg not in captured.out
