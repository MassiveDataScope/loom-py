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
