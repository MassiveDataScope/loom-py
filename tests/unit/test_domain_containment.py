"""Domain containment lint for the row-scoped schema mechanism (FR-002, FR-037).

loom ships the mechanism; the product supplies every name. These tests fail on
any domain word, default column name, default session key, default database
user name or environment label inside the packages this feature adds, inside
the SQL the bootstrap renders for a synthetic configuration, and inside the
session-settings module the mechanism builds on.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[2] / "src" / "loom"

_FILES = (
    "core/model/privilege.py",
    "core/model/scoped.py",
    "core/model/introspection.py",
    "core/backend/scoped_ddl.py",
    "core/authz/elevation.py",
    "core/authz/product.py",
    "core/locator.py",
    "core/repository/sqlalchemy/session_settings.py",
)
_PACKAGES = (
    "core/repository/sqlalchemy/rls",
    "core/repository/sqlalchemy/migrations",
)

_DOMAIN_WORDS = re.compile(
    r"\b("
    r"tenants?|multi[- ]?tenant|organi[sz]ations?|org_id|customers?|compan(y|ies)"
    r"|workspaces?|project_id|account_id|owner_id|region"
    r"|prod|production|staging|periplo"
    r")\b",
    re.IGNORECASE,
)
_RETIRED_NAMES = re.compile(
    r"\b(TenantScoped|tenant_key|scope_key|grantees|Grantee|GrantWriter|write_registry"
    r"|retarget|table_grant|guard_version|StrictChecks)\b"
)
_DEFAULT_LITERALS = re.compile(r"""['"](app\.\w+|tenant_id|app_owner|app_platform|app)['"]""")


def _existing() -> Iterator[Path]:
    for rel in _FILES:
        path = _SRC / rel
        if path.exists():
            yield path
    for rel in _PACKAGES:
        yield from sorted((_SRC / rel).glob("**/*.py"))


def _sources() -> list[Path]:
    missing = [rel for rel in _FILES if not (_SRC / rel).exists()]
    missing += [rel for rel in _PACKAGES if not (_SRC / rel).is_dir()]
    if missing:
        pytest.fail(f"feature modules not present yet: {missing}")
    return list(_existing())


def _offending_lines(text: str, pattern: re.Pattern[str]) -> list[str]:
    return [line.strip() for line in text.splitlines() if pattern.search(line)]


def test_feature_modules_carry_no_domain_vocabulary() -> None:
    offenders = {
        str(path.relative_to(_SRC)): hits
        for path in _sources()
        if (hits := _offending_lines(path.read_text(), _DOMAIN_WORDS))
    }

    assert offenders == {}


def test_feature_modules_carry_no_retired_names() -> None:
    offenders = {
        str(path.relative_to(_SRC)): hits
        for path in _sources()
        if (hits := _offending_lines(path.read_text(), _RETIRED_NAMES))
    }

    assert offenders == {}


def test_feature_modules_bake_no_product_name_as_a_literal() -> None:
    offenders = {
        str(path.relative_to(_SRC)): hits
        for path in _sources()
        if (hits := _offending_lines(path.read_text(), _DEFAULT_LITERALS))
    }

    assert offenders == {}


def _dataclass_defaults(path: Path, class_name: str) -> dict[str, bool]:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {
                field.target.id: field.value is not None
                for field in node.body
                if isinstance(field, ast.AnnAssign) and isinstance(field.target, ast.Name)
            }
    pytest.fail(f"{class_name} not found in {path}")


@pytest.mark.parametrize(
    ("class_name", "fields_without_default"),
    [
        ("BootstrapConfig", {"schema", "roles", "database_users"}),
        ("DatabaseRoles", {"owner", "migrator"}),
        ("DatabaseUser", {"login", "access"}),
    ],
)
def test_bootstrap_types_have_no_name_defaults(
    class_name: str, fields_without_default: set[str]
) -> None:
    path = _SRC / "core/repository/sqlalchemy/rls/bootstrap.py"
    if not path.exists():
        pytest.fail(f"{path} does not exist yet")

    defaults = _dataclass_defaults(path, class_name)

    assert {name for name in fields_without_default if defaults.get(name, True)} == set()


def test_rendered_bootstrap_contains_only_injected_names() -> None:
    from loom.core.repository.sqlalchemy.rls import (
        BootstrapConfig,
        DatabaseRoles,
        DatabaseUser,
        render_bootstrap,
    )

    sql = render_bootstrap(
        BootstrapConfig(
            schema="s1",
            roles=DatabaseRoles(owner="r_owner", migrator="r_migrator"),
            database_users={
                "u_read": DatabaseUser(login=True, access="read"),
                "u_write": DatabaseUser(login=True, access="write"),
                "u_bypass": DatabaseUser(login=True, access="bypass"),
            },
        )
    )

    assert _offending_lines(sql, _DOMAIN_WORDS) == []
    assert _offending_lines(sql, _DEFAULT_LITERALS) == []
    for injected in ("s1", "r_owner", "r_migrator", "u_read", "u_write", "u_bypass"):
        assert injected in sql
