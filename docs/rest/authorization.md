# Role-based authorization

{class}`~loom.core.identity.Identity` says *who* is calling. `loom.core.authz` answers
the next question, *may they do this here?*, without deciding for you what "here"
means. Your product declares its permissions and roles in code, stores who holds which
role where, and asks pure functions for decisions.

```{contents}
:local:
:depth: 2
```

---

## The model

| Piece | Lives in | What it is |
|-------|----------|------------|
| {class}`~loom.core.authz.Permission` | your code | a capability, such as `"catalog.read"` |
| {class}`~loom.core.authz.Role` | your code | a named set of permissions |
| {class}`~loom.core.authz.RoleCatalog` | your code | every permission and role, validated once |
| {class}`~loom.core.authz.Scope` | your data | where a grant applies: a path of segments |
| {class}`~loom.core.authz.Grant` | your database | subject + role **name** + scope, optionally until an instant |

A grant stores the role's name, never its permissions, so changing a role in code
changes what every stored grant of it allows. The token only identifies the caller;
it never carries permissions.

loom implements Core RBAC (ANSI/INCITS 359) without role hierarchies or
separation-of-duty constraints; hierarchy is expressed through permission sets, and
separation of duty belongs to the product.

## Scopes are paths

loom gives the segments no meaning. A product with workspaces, projects and datasets
might use `Scope.of("acme")`, `Scope.of("acme", "billing")` and
`Scope.of("acme", "billing", "invoices")`; another might put a region first. A grant on a
scope reaches that scope and everything below it, and nothing else:

| grant on \ access to | `/` | `/W` | `/W/x` | `/W/x/y` | `/W/z` | `/V` |
|---|---|---|---|---|---|---|
| `/` (`Scope.root()`) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `/W` | — | ✓ | ✓ | ✓ | ✓ | — |
| `/W/x` | — | — | ✓ | ✓ | — | — |
| `/W/x/y` | — | — | — | ✓ | — | — |
| `/V` | — | — | — | — | — | ✓ |

The root is where operators of the whole product hold their roles. Segments are
non-empty and may not contain `/`, so `str(scope)` (`/`, `/acme/billing`) is
unambiguous. {meth}`~loom.core.authz.Scope.parse` reads it back, and that text form is
stable, so it is what you store in a grants table.

## Declaring roles

```python
from loom.core.authz import Permission, Role, RoleCatalog

READ = Permission("catalog.read")
RUN = Permission("jobs.run")
MANAGE = Permission("members.manage")

CATALOG = RoleCatalog(
    [READ, RUN, MANAGE],
    [
        Role("viewer", {READ}),
        Role("operator", {READ, RUN}),
        Role("admin", {READ, RUN, MANAGE}),
        Role("helpdesk", {READ, MANAGE}),
    ],
)
```

The catalog refuses a duplicate role name and a role that uses a permission you did not
declare, and every decision refuses a permission the catalog does not declare
({class}`~loom.core.authz.UnknownPermission`), so a typo fails loudly rather than
silently denying. Roles only add:
there are no negative permissions, and holding two roles gives their union.

## Deciding

```python
from loom.core.authz import Grant, Scope, evaluate, scopes_with

grants = [
    Grant("ada", "viewer", Scope.of("acme")),
    Grant("ada", "operator", Scope.of("acme", "billing")),
]

evaluate(CATALOG, grants, READ, Scope.of("acme", "billing", "invoices"))   # allowed
evaluate(CATALOG, grants, RUN, Scope.of("acme"))                           # denied
evaluate(CATALOG, grants, READ, Scope.of("globex"))                        # denied
```

{func}`~loom.core.authz.evaluate` never touches a database or reads a clock. It denies unless
some grant names a role that includes the permission on a covering scope. The
{class}`~loom.core.authz.Decision` it returns names that grant for your audit log; when
several grants allow, it names the one on the deepest scope (then the first by role name
and subject, then the longest-lived), so the log does not depend on the order your store returned the rows in.
A stored grant whose role the code does not declare allows nothing.

To filter a listing, ask once for the outermost scopes and test each item by prefix:

```python
reachable = scopes_with(CATALOG, grants, READ)      # frozenset({Scope.of("acme")})
visible = [item for item in items if any(scope.covers(item.scope) for scope in reachable)]
```

## Time-bound grants

Privileged and temporary access, such as an on-call operator or a contractor, should end
on its own rather than wait for someone to remember to revoke it. Give the grant a
timezone-aware `expires_at` and pass the decision instant as `now=`:

```python
from datetime import UTC, datetime, timedelta

now = datetime.now(UTC)
on_call = [Grant("ada", "operator", Scope.of("acme"), expires_at=now + timedelta(hours=8))]

evaluate(CATALOG, on_call, RUN, Scope.of("acme"), now=now)                        # allowed
evaluate(CATALOG, on_call, RUN, Scope.of("acme"), now=now + timedelta(hours=8))   # denied
```

A grant is alive while `now` is before `expires_at`; from that instant on it counts as
absent: it allows nothing, {func}`~loom.core.authz.scopes_with` leaves its scope out, and
{func}`~loom.core.authz.can_grant` and {func}`~loom.core.authz.can_revoke` give the
granter no power through it. loom still reads no clock, so the caller chooses `now` and a
test can pin it. Grants without `expires_at` never need `now`; if any grant passed in
can expire and `now` is missing, the call raises `ValueError` instead of guessing, and a
naive `expires_at` or `now` raises too. Revoking a grant that has already expired
follows the usual rules.

## Nobody grants more than they hold

{func}`~loom.core.authz.can_grant` takes the permission that allows assigning roles at all
(`delegate=`, here `members.manage`) and requires two things on the target scope: the
granter holds `delegate`, and the granter holds every permission of the role being handed
out. The answer lists whatever is missing.

```python
from loom.core.authz import can_grant, can_revoke

manager = [Grant("bob", "admin", Scope.of("acme"))]
can_grant(CATALOG, manager, "operator", Scope.of("acme", "billing"), delegate=MANAGE)  # allowed
can_grant(CATALOG, manager, "admin", Scope.root(), delegate=MANAGE)                     # denied

support = [Grant("eve", "helpdesk", Scope.of("acme"))]
can_grant(CATALOG, support, "admin", Scope.of("acme"), delegate=MANAGE).missing
# frozenset({Permission("jobs.run")})

operator = [Grant("ola", "operator", Scope.of("acme"))]
can_grant(CATALOG, operator, "viewer", Scope.of("acme"), delegate=MANAGE).missing
# frozenset({Permission("members.manage")})
```

Holding `members.manage`, as `helpdesk` does, is not enough to promote someone to `admin`,
and holding a role is not enough to hand it out. Revoking needs exactly what granting
would ({func}`~loom.core.authz.can_revoke`); a stored grant of a role the code does not
declare allows nothing, so removing it needs `delegate` only. Asking to grant an
undeclared role raises {class}`~loom.core.authz.UnknownRole`.

## Where grants come from

Anything with an `async def grants_for(self, subject: str)` method is a
{class}`~loom.core.authz.GrantSource`: a SQL table, a cache in front of it, a file.
loom does not store, cache or invalidate grants; that is your product's policy.
{class}`~loom.core.authz.InMemoryGrantSource` serves tests and examples.

```python
from loom.core.authz import InMemoryGrantSource

source = InMemoryGrantSource(grants)
decision = evaluate(CATALOG, await source.grants_for(identity.require_subject()), READ, scope)
```

## Roles as data

A product that keeps its role composition outside code declares each role as a
{class}`~loom.core.authz.RoleSpec`: the permission names it adds and the roles it
extends. {meth}`~loom.core.authz.RoleCatalog.from_specs` flattens the extensions:

```python
from loom.core.authz import RoleCatalog, RoleSpec

CATALOG = RoleCatalog.from_specs(
    [READ, RUN, MANAGE],
    {
        "viewer": RoleSpec(permissions=frozenset({"catalog.read"})),
        "operator": RoleSpec(permissions=frozenset({"jobs.run"}), extends=("viewer",)),
        "admin": RoleSpec(permissions=frozenset({"members.manage"}), extends=("operator",)),
    },
)
```

Each role holds the union of its own permissions and those of every role it extends, so
this catalog equals one built by hand from the flattened roles. Building fails with
`ValueError` naming the role and the cause:

| Cause | Example |
|---|---|
| a cycle | `viewer` extends `admin`, which extends `viewer` |
| an unknown base | `extends=("veiwer",)` |
| an undeclared permission | `permissions=frozenset({"catalog.write"})` |

loom reads no file. The product parses its own format, per environment if it wants, and
passes the specs.

### Catalog digest

{meth}`~loom.core.authz.RoleCatalog.digest` returns a SHA-256 over the sorted
`role:perm,perm` lines of the catalog. It does not depend on declaration order and
changes with any permission of any role, custom roles included. Record it next to a
deployment, or compare it in CI, to notice a catalog that changed between environments.

```python
CATALOG.digest()
```

## Custom roles

{meth}`~loom.core.authz.RoleCatalog.compose` adds roles defined by a product's
administrators to a fixed base catalog, under rules the product sets in
{class}`~loom.core.authz.CompositionRules`. Every field is required:

| Field | Meaning |
|---|---|
| `custom_prefix` | prefix of every custom role name |
| `may_extend_base` | whether a custom role may extend a base role at all |
| `non_extensible` | base roles no custom role may extend |
| `ceiling` | the most a custom role may hold; never `None` |

The ceiling is the creator's effective permissions at the scope where the role is
created, so nobody defines a role above what they hold:

```python
from loom.core.authz import CompositionRules, RoleSpec, Scope, evaluate

at = Scope.of("acme")
ceiling = frozenset(
    permission
    for permission in CATALOG.permissions
    if evaluate(CATALOG, creator_grants, permission, at)
)
rules = CompositionRules(
    custom_prefix="custom:",
    may_extend_base=True,
    non_extensible=frozenset({"admin"}),
    ceiling=ceiling,
)
effective = RoleCatalog.compose(
    CATALOG,
    {"runner": RoleSpec(permissions=frozenset({"jobs.run"}), extends=("viewer",))},
    rules,
    namespace="acme",
)
effective.role("custom:acme/runner")
```

The effective name of a custom role is `f"{custom_prefix}{namespace}/{name}"`. The
`namespace` names where the role is defined, so two boundaries that each create a
`runner` obtain two different roles and neither can be granted in the other. The base
roles are kept unchanged. Composition fails with `ValueError` naming the offender when:

- `namespace` or a custom name is empty or contains `/`;
- a custom role uses a permission the base catalog does not declare;
- a custom role, once flattened, holds a permission outside `ceiling`;
- a custom role extends a base role while `may_extend_base` is false, or extends a role
  in `non_extensible`;
- an effective name collides with a base role.

loom does not persist custom roles; storing, caching and auditing them is the product's
job. Grant a custom role by its effective name and decide with the effective catalog.

### Editing a granted custom role

Changing the permissions of a custom role changes what every existing grant of it
allows. Before applying the edit, check every holder's grant against the new catalog
with the editor's grants, and refuse on any failure (obligation P3):

```python
from loom.core.authz import can_grant

for grant in holders_of_role:
    check = can_grant(new_catalog, editor_grants, grant.role, grant.scope, delegate=MANAGE)
    if not check:
        raise PermissionError(f"edit refused on {grant.scope}: missing {sorted(check.missing)}")
```

## The product declaration

A product publishes what loom needs to decide, delegate and elevate as an
{class}`~loom.core.authz.product.AuthzProduct`: `catalog`, `delegate` (the permission
{func}`~loom.core.authz.can_grant` and {func}`~loom.core.authz.can_revoke` require of a
granter) and `elevations` (elevable scope name to permission):

```python
EDIT_ANY = Permission("entries.edit_any")


class LedgerAuthz:
    catalog = CATALOG
    delegate = MANAGE
    elevations = {"clerk": EDIT_ANY}
```

Register it under the `loom.authz` entry point group, as the object or a factory that
returns it:

```toml
[project.entry-points."loom.authz"]
ledger = "ledger.authz:LedgerAuthz"
```

{func}`~loom.core.authz.product.load_authz_product` returns the product, or `None` when
none is registered; {func}`~loom.core.authz.product.register_authz_product` installs one
for the process ahead of the entry point, which is what tests use.
{func}`~loom.core.repository.sqlalchemy.rls.validate_elevations` raises `ConfigError` when
`elevations` names a scope that no compiled model declares, or one that is not elevable.
The standard SQLAlchemy backend calls it at startup, with the compiled scoped tables,
when `database.schema.mode` is `external` on Postgres; it remains available for
programmatic use.

## Elevating a write scope

A [row-scoped table](postgres-rls.md#row-scoped-tables) can declare a write-only,
elevable scope: callers read every row within the boundary but modify only their own.
When the product's authorizer allows more, for example an administrator editing other
users' rows, {func}`~loom.core.repository.sqlalchemy.rls.elevate` turns that decision,
and only that decision, into the session flag `loom.scope.<scope>.any` for the rest of
the current execution:

```python
from loom.core.authz import Scope, evaluate
from loom.core.repository.sqlalchemy.rls import elevate

at = Scope.of(account_id)
decision = evaluate(CATALOG, grants, EDIT_ANY, at)
if not decision:
    raise PermissionError("not allowed to edit other clerks' entries")
await elevate("clerk", decision, at=at)
```

`elevate(scope, decision, *, at, catalog=None)` has one error per cause and sets nothing
when it raises. Outside an execution it raises `RuntimeError` before checking anything
else:

| Error | Cause |
|---|---|
| `RuntimeError` | no use-case execution is running |
| `ConfigError` | no product is registered, or `scope` is not in `AuthzProduct.elevations` |
| `PermissionError` | the decision is denied, its grant's role is not in `catalog.roles_with(elevations[scope])`, or its grant's scope does not cover `at` |

`catalog` defaults to `AuthzProduct.catalog`; a product with custom roles passes its
effective catalog.

### One execution, no more

The executor opens an elevation frame for every use-case execution, around the pipeline
and the commit or rollback of its unit of work. The frame decides how long an elevation
lives:

- the flag is transaction-local: the session-settings provider emits `'on'` in every
  transaction that begins while the frame holds the scope, and `''` otherwise;
- elevating after a read in the same unit of work also flags the open transaction, through
  the executor's elevation sink,
  {class}`~loom.core.repository.sqlalchemy.rls.SQLAlchemyElevationSink`; the standard
  SQLAlchemy backend provides it in `external` mode on Postgres through
  `PersistenceWiring.elevation_sink`, and a kernel built by hand passes it as
  `create_kernel(elevation_sink=...)`;
- a nested execution inherits its parent's elevations, never changes them, and clears
  on exit only what it added; when that clearing fails, the session is invalidated, so
  no flag outlives the execution that set it;
- a task spawned during the execution shares the frame and loses the elevation when the
  execution ends;
- post-commit actions and the next execution on the same pooled connection are not
  elevated.

{func}`~loom.core.authz.elevation.elevated_scopes` returns the scopes elevated in the
current context, empty outside an execution.

### Elevate only at the boundary

`elevate` checks that the decision's grant covers `at`. It does not check that `at` is
the boundary the session is filtered by, and the flag opens every row of the elevable
scope within that boundary. Call `elevate` only with a decision evaluated at the scope of
the boundary (obligation P2); a decision taken on a narrower scope would open more than
it justifies.
