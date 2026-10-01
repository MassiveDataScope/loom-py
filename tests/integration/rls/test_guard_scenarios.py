"""The gate evidence replayed against the bootstrap loom renders (T013).

Three synthetic products share one database. Every label below is a line of
``gate-evidence/fixture-round7.tail.sql``; the SQL is the fixture's, the guard
is the one ``apply_bootstrap`` installed. Scenarios keep the fixture's order
because some of them commit state the next ones rely on.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    apply_bootstrap,
)

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
SCHEMAS = ("notes", "sites", "ledger")
SEED = Path(__file__).with_name("guard_seed.sql")
RW = "'SELECT','INSERT','UPDATE','DELETE'"
SELECT_ONLY = "'SELECT'"
OWNER = '[{"col":"owner_id","scope":"owner","on":"both"}]'
REGION = '[{"col":"region","scope":"region","on":"both"}]'
KIND = '[{"col":"id","scope":"k","on":"both"}]'
QUAL = "owner_id = NULLIF(current_setting('loom.scope.owner', true), '')::uuid"
DENY = "loom_guard_notes.deny_owner_dml()"
ALL_EVENTS = "INSERT OR UPDATE OR DELETE OR TRUNCATE"
ASSERT_NOTES = "SELECT loom_guard_notes.assert_scoped_schema()"


@dataclass(frozen=True, slots=True)
class Guarded:
    dsn: str

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[asyncpg.Connection]:
        conn = await asyncpg.connect(self.dsn)
        try:
            yield conn
        finally:
            await conn.close()


def _config(schema: str) -> BootstrapConfig:
    users = {
        f"{schema}_ro": DatabaseUser(login=True, access="read"),
        f"{schema}_rw": DatabaseUser(login=True, access="write"),
        f"{schema}_ops": DatabaseUser(login=True, access="bypass"),
    }
    return BootstrapConfig(
        schema=schema,
        roles=DatabaseRoles(owner=f"{schema}_owner", migrator=f"{schema}_migrator"),
        database_users=users,
    )


async def _seed(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(SEED.read_text())
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def guarded(module_database_uri: str, created_roles: set[str]) -> Iterator[Guarded]:
    suffixes = ("owner", "migrator", "readers", "writers", "ro", "rw", "ops")
    for schema in SCHEMAS:
        created_roles.update(f"{schema}_{suffix}" for suffix in suffixes)
        asyncio.run(apply_bootstrap(module_database_uri, _config(schema), passwords={}))
    created_roles.add("notes_new")
    url = make_url(module_database_uri).set(drivername="postgresql")
    dsn = url.render_as_string(hide_password=False)
    asyncio.run(_seed(dsn))
    yield Guarded(dsn)


async def _sqlstate(conn: asyncpg.Connection, statement: str) -> str:
    try:
        await conn.execute(statement)
    except asyncpg.PostgresError as exc:
        return exc.sqlstate
    return "none"


async def _run(
    guarded: Guarded,
    *,
    role: str | None = None,
    prelude: Sequence[str] = (),
    statement: str,
    commit: bool = False,
) -> str:
    async with guarded.connection() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            if role is not None:
                await conn.execute(f"SET LOCAL ROLE {role}")
            for step in prelude:
                await conn.execute(step)
            got = await _sqlstate(conn, statement)
        finally:
            if commit and got == "none":
                await tx.commit()
            else:
                await tx.rollback()
        return got


async def _truth(
    guarded: Guarded, *, role: str | None = None, prelude: Sequence[str] = (), check: str
) -> bool:
    """Evaluate ``check`` after ``prelude`` and roll everything back, as the fixture did."""
    async with guarded.connection() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            if role is not None:
                await conn.execute(f"SET LOCAL ROLE {role}")
            for step in prelude:
                await conn.execute(step)
            return bool(await conn.fetchval(f"SELECT ({check})"))
        finally:
            await tx.rollback()


def _keys(owner: str = "", editor: str = "", any_: str = "") -> str:
    return (
        f"SELECT set_config('loom.scope.owner', '{owner}', true), "
        f"set_config('loom.scope.editor', '{editor}', true), "
        f"set_config('loom.scope.editor.any', '{any_}', true)"
    )


def _owner_key(owner: str) -> str:
    return f"SELECT set_config('loom.scope.owner', '{owner}', true)"


def _hatch(schema: str) -> str:
    return f"SELECT set_config('loom_guard_{schema}.protecting', 'on', true)"


def _protect(schema: str, table: str, scopes: str, privileges: str) -> str:
    return (
        f"SELECT loom_guard_{schema}.protect_scoped_table"
        f"('{table}', '{scopes}', ARRAY[{privileges}])"
    )


def _note(owner: str, editor: str, body: str) -> str:
    return (
        f"INSERT INTO notes.notes (owner_id, editor, body) VALUES ('{owner}','{editor}','{body}')"
    )


def _policy(name: str, kind: str, roles: str, predicate: str, *, restrictive: bool = False) -> str:
    mode = " AS RESTRICTIVE" if restrictive else ""
    return f"CREATE POLICY {name} ON notes.notes{mode} FOR {kind} TO {roles} {predicate}"


def _trigger(events: str, function: str, when: str = "") -> list[str]:
    return [
        "DROP TRIGGER loom_deny_owner_dml ON notes.notes",
        f"CREATE TRIGGER loom_deny_owner_dml BEFORE {events} ON notes.notes "
        f"FOR EACH STATEMENT {when}EXECUTE FUNCTION {function}",
        "ALTER TABLE notes.notes ENABLE ALWAYS TRIGGER loom_deny_owner_dml",
    ]


def _scoped(table: str) -> str:
    return f"CREATE TABLE {table} (owner_id uuid NOT NULL, id serial, PRIMARY KEY (owner_id, id))"


def _pk_only(table: str) -> str:
    return f"CREATE TABLE {table} (owner_id uuid NOT NULL, id serial PRIMARY KEY)"


async def _attempt_as(conn: asyncpg.Connection, role: str, statement: str) -> str:
    """Run ``statement`` as ``role`` inside a savepoint so the outer transaction survives."""
    try:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL ROLE {role}")
            await conn.execute(statement)
    except asyncpg.PostgresError as exc:
        return exc.sqlstate
    return "none"


def _count(table: str) -> str:
    return f"(SELECT count(*) FROM {table})"


def _all_assertions() -> str:
    return "SELECT " + ", ".join(f"loom_guard_{s}.assert_scoped_schema()" for s in SCHEMAS)


async def test_a0_three_schemas_rendered_from_one_template_all_assertions_pass(
    guarded: Guarded,
) -> None:
    assert await _run(guarded, statement=_all_assertions()) == "none"


class TestCombinationAndElevation:
    async def test_g1_1_read_sees_only_own_boundary(self, guarded: Guarded) -> None:
        keys = [_keys(U1, "ana")]
        assert await _truth(
            guarded, role="notes_rw", prelude=keys, check=f"{_count('notes.notes')} = 2"
        )

    @pytest.mark.parametrize(
        ("label", "statement"),
        [
            ("G1.3 cross-boundary INSERT with own editor", _note(U2, "ana", "x")),
            ("G1.4 own boundary INSERT with a foreign editor", _note(U1, "bob", "y")),
            (
                "G1.6 UPDATE moving a row out of the boundary",
                f"UPDATE notes.notes SET owner_id = '{U2}' WHERE editor = 'ana'",
            ),
        ],
    )
    async def test_writes_outside_the_boundary_are_refused(
        self, guarded: Guarded, label: str, statement: str
    ) -> None:
        got = await _run(guarded, role="notes_rw", prelude=[_keys(U1, "ana")], statement=statement)
        assert got == "42501", label

    async def test_g1_5_own_upsert_works_with_the_serial_sequence_granted(
        self, guarded: Guarded
    ) -> None:
        upsert = (
            _note(U1, "ana", "dup")
            + " ON CONFLICT (owner_id, editor) DO UPDATE SET body = 'ana-upsert'"
        )
        check = "(SELECT body FROM notes.notes WHERE editor='ana') = 'ana-upsert'"
        assert await _truth(
            guarded, role="notes_rw", prelude=[_keys(U1, "ana"), upsert], check=check
        )

    async def test_g2_1_non_elevated_update_of_another_editor_affects_zero_rows(
        self, guarded: Guarded
    ) -> None:
        update = "UPDATE notes.notes SET body = 'hacked' WHERE editor = 'bob'"
        check = "(SELECT count(*) FROM notes.notes WHERE body='hacked') = 0"
        assert await _truth(
            guarded, role="notes_rw", prelude=[_keys(U1, "ana"), update], check=check
        )

    async def test_g2_2_elevated_update_inside_the_boundary_affects_one_row(
        self, guarded: Guarded
    ) -> None:
        update = "UPDATE notes.notes SET body = 'edited-by-admin' WHERE editor = 'bob'"
        check = "(SELECT count(*) FROM notes.notes WHERE body='edited-by-admin') = 1"
        prelude = [_keys(U1, "ana", "on"), update]
        assert await _truth(guarded, role="notes_rw", prelude=prelude, check=check)

    async def test_g2_4_elevation_never_widens_reads(self, guarded: Guarded) -> None:
        leak = "UPDATE notes.notes SET body = 'leak' WHERE editor = 'zoe'"
        prelude = [_keys(U1, "ana", "on"), leak]
        assert await _truth(
            guarded, role="notes_rw", prelude=prelude, check=f"{_count('notes.notes')} = 2"
        )

    async def test_g2_3_elevated_update_outside_the_boundary_changed_nothing(
        self, guarded: Guarded
    ) -> None:
        leak = "UPDATE notes.notes SET body = 'leak' WHERE editor = 'zoe'"
        prelude = [_keys(U1, "ana", "on"), leak, "SET LOCAL ROLE notes_ops"]
        check = "(SELECT body FROM notes.notes WHERE editor='zoe') = 'zoe-2'"
        assert await _truth(guarded, role="notes_rw", prelude=prelude, check=check)

    async def test_g2_5_elevated_insert_outside_the_boundary_is_refused(
        self, guarded: Guarded
    ) -> None:
        prelude = [_keys(U1, "ana", "on")]
        got = await _run(guarded, role="notes_rw", prelude=prelude, statement=_note(U2, "zoe", "z"))
        assert got == "42501"

    async def test_g1_7_no_boundary_value_means_zero_rows_even_when_elevated(
        self, guarded: Guarded
    ) -> None:
        prelude = [_keys("", "", "on")]
        assert await _truth(
            guarded, role="notes_rw", prelude=prelude, check=f"{_count('notes.notes')} = 0"
        )

    async def test_g1_8_a_malformed_boundary_fails_closed(self, guarded: Guarded) -> None:
        prelude = [_owner_key("not-a-uuid")]
        got = await _run(
            guarded, role="notes_rw", prelude=prelude, statement="SELECT count(*) FROM notes.notes"
        )
        assert got == "22P02"

    async def test_g9_a_readers_only_user_reads_but_cannot_write(self, guarded: Guarded) -> None:
        keys = [_owner_key(U1)]
        assert await _truth(
            guarded, role="notes_ro", prelude=keys, check=f"{_count('notes.notes')} = 2"
        )
        update = "UPDATE notes.notes SET body='r' WHERE editor='ana'"
        assert await _run(guarded, role="notes_ro", prelude=keys, statement=update) == "42501"


class TestPoolResidue:
    async def test_g3_every_key_reemitted_overrides_a_session_residue_and_reset_all_clears_it(
        self, guarded: Guarded
    ) -> None:
        residue = (
            f"SELECT set_config('loom.scope.owner', '{U1}', false), "
            "set_config('loom.scope.editor.any', 'on', false)"
        )
        async with guarded.connection() as conn:
            await conn.execute("SET ROLE notes_rw")
            await conn.execute(residue)
            async with conn.transaction():
                await conn.execute(_keys())
                assert await conn.fetchval("SELECT count(*) FROM notes.notes") == 0
                flag = await conn.fetchval("SELECT current_setting('loom.scope.editor.any', true)")
                assert flag == ""
            await conn.execute("RESET ALL")
            cleared = "SELECT coalesce(current_setting('loom.scope.editor.any', true), '')"
            assert await conn.fetchval(cleared) == ""


class TestForeignKeysCascadeAndOwnerDml:
    async def test_g5_2_a_composite_fk_rejects_a_cross_boundary_reference(
        self, guarded: Guarded
    ) -> None:
        insert = f"INSERT INTO notes.note_items (owner_id, note_id, name) VALUES ('{U1}', 3, 'x')"
        prelude = [_keys(U1, "ana", "on")]
        assert await _run(guarded, role="notes_rw", prelude=prelude, statement=insert) == "23503"
        check = f"(SELECT owner_id::text FROM notes.notes WHERE id = 3) = '{U2}'"
        assert await _truth(guarded, role="notes_ops", check=check)

    @pytest.mark.parametrize("role", ["notes_rw", "notes_ops"])
    async def test_c1_cascade_completes_through_the_enable_always_owner_trigger(
        self, guarded: Guarded, role: str
    ) -> None:
        prelude = [_keys(U1, "ana", "on"), "DELETE FROM notes.notes WHERE editor = 'bob'"]
        check = "(SELECT count(*) FROM notes.note_items WHERE name='item-bob') = 0"
        assert await _truth(guarded, role=role, prelude=prelude, check=check)

    @pytest.mark.parametrize(
        ("label", "statement", "want"),
        [
            ("C1.3 owner DML", "DELETE FROM notes.notes WHERE editor='ana'", "LG001"),
            ("C1.4 owner TRUNCATE", "TRUNCATE notes.note_items", "LG001"),
            (
                "C1.5 disabling the ENABLE ALWAYS trigger",
                "ALTER TABLE notes.notes DISABLE TRIGGER loom_deny_owner_dml",
                "LG002",
            ),
        ],
    )
    async def test_the_owner_cannot_touch_rows_or_the_trigger(
        self, guarded: Guarded, label: str, statement: str, want: str
    ) -> None:
        assert await _run(guarded, role="notes_owner", statement=statement) == want, label

    async def test_c1_3_the_owner_dml_error_names_the_schema(self, guarded: Guarded) -> None:
        async with guarded.connection() as conn, conn.transaction():
            await conn.execute("SET LOCAL ROLE notes_owner")
            with pytest.raises(asyncpg.PostgresError) as failure:
                await conn.execute("DELETE FROM notes.notes WHERE editor='ana'")
        assert failure.value.sqlstate == "LG001"
        assert "loom_guard[notes]" in str(failure.value)

    async def test_g5_7_truncate_is_denied_to_the_app(self, guarded: Guarded) -> None:
        assert await _run(guarded, role="notes_rw", statement="TRUNCATE notes.notes") == "42501"

    @pytest.mark.parametrize("table", ["notes.note_events", "notes.note_events_2026"])
    async def test_g5_partitioned_parent_and_partition_are_filtered(
        self, guarded: Guarded, table: str
    ) -> None:
        assert await _truth(
            guarded, role="notes_rw", prelude=[_owner_key(U1)], check=f"{_count(table)} = 1"
        )


class TestDecisionClosures:
    async def test_d1_an_untagged_ddl_is_still_caught(self, guarded: Guarded) -> None:
        statement = "ALTER TRIGGER loom_deny_owner_dml ON notes.notes RENAME TO x"
        assert await _run(guarded, role="notes_owner", statement=statement) == "LG002"

    @pytest.mark.parametrize(
        ("label", "grant"),
        [
            ("D2a to the other product's bypass", "SELECT ON notes.notes TO sites_ops"),
            ("D2b directly to a user", "SELECT ON notes.notes TO notes_rw"),
            ("D2c INSERT to readers", "INSERT ON notes.notes TO notes_readers"),
            ("D2d TRUNCATE to the declared bypass", "TRUNCATE ON notes.notes TO notes_ops"),
            ("D2e to PUBLIC", "SELECT ON notes.notes TO PUBLIC"),
            ("D2f a column grant", "UPDATE (body) ON notes.notes TO notes_readers"),
        ],
    )
    async def test_d2_grants_outside_the_whitelist_are_rejected(
        self, guarded: Guarded, label: str, grant: str
    ) -> None:
        assert await _run(guarded, role="notes_owner", statement=f"GRANT {grant}") == "LG002", label

    async def test_d3b_protect_refuses_a_table_with_hand_made_policies(
        self, guarded: Guarded
    ) -> None:
        prelude = [
            _hatch("notes"),
            _scoped("notes.laundry"),
            "CREATE POLICY loom_select ON notes.laundry FOR SELECT TO PUBLIC USING (true)",
        ]
        statement = _protect("notes", "notes.laundry", OWNER, SELECT_ONLY)
        assert (
            await _run(guarded, role="notes_owner", prelude=prelude, statement=statement) == "42501"
        )

    async def test_d3c_d3d_a_foreign_session_user_with_execute_still_fails_ownership(
        self, guarded: Guarded
    ) -> None:
        functions = "loom_guard_notes.protect_scoped_table, loom_guard_notes.unprotect_scoped_table"
        grants = [
            "GRANT USAGE ON SCHEMA notes TO sites_owner",
            "GRANT USAGE ON SCHEMA loom_guard_notes TO sites_owner",
            f"GRANT EXECUTE ON FUNCTION {functions} TO sites_owner",
        ]
        revokes = [
            f"REVOKE EXECUTE ON FUNCTION {functions} FROM sites_owner",
            "REVOKE USAGE ON SCHEMA loom_guard_notes FROM sites_owner",
            "REVOKE USAGE ON SCHEMA notes FROM sites_owner",
        ]
        protect = _protect("notes", "notes.note_kinds", KIND, SELECT_ONLY)
        unprotect = "SELECT loom_guard_notes.unprotect_scoped_table('notes.notes')"
        async with guarded.connection() as conn:
            for grant in grants:
                await conn.execute(grant)
            try:
                await conn.execute("SET SESSION AUTHORIZATION sites_owner")
                assert await _sqlstate(conn, protect) == "42501"
                assert await _sqlstate(conn, unprotect) == "42501"
            finally:
                await conn.execute("RESET SESSION AUTHORIZATION")
                for revoke in revokes:
                    await conn.execute(revoke)

    @pytest.mark.parametrize(
        ("label", "scopes", "privileges", "want"),
        [
            (
                "D4a unknown column",
                '[{"col":"nope","scope":"k","on":"both"}]',
                SELECT_ONLY,
                "22023",
            ),
            ("D4b unknown reach", '[{"col":"id","scope":"k","on":"bth"}]', SELECT_ONLY, "22023"),
            (
                "D4c bad scope name",
                '[{"col":"id","scope":"k-1","on":"both"}]',
                SELECT_ONLY,
                "22023",
            ),
            (
                "D5a two boundaries",
                '[{"col":"id","scope":"a","on":"both"},{"col":"name","scope":"b","on":"both"}]',
                SELECT_ONLY,
                "22023",
            ),
            (
                "D5b only an elevable scope",
                '[{"col":"name","scope":"k","on":"write","elevable":true}]',
                SELECT_ONLY,
                "22023",
            ),
            ("P3 privilege outside the set", KIND, "'SELECT','TRUNCATE'", "22023"),
            ("P4 empty scopes", "[]", SELECT_ONLY, "22023"),
            ("P4b scopes not an array", '{"col":"id"}', SELECT_ONLY, "22023"),
            (
                "P5 elevable scope not write-only",
                '[{"col":"id","scope":"k","on":"both"},'
                '{"col":"name","scope":"n","on":"both","elevable":true}]',
                SELECT_ONLY,
                "22023",
            ),
        ],
    )
    async def test_protect_rejects_an_invalid_declaration(
        self, guarded: Guarded, label: str, scopes: str, privileges: str, want: str
    ) -> None:
        statement = _protect("notes", "notes.note_kinds", scopes, privileges)
        assert await _run(guarded, role="notes_owner", statement=statement) == want, label

    @pytest.mark.parametrize(
        ("label", "columns", "action"),
        [
            ("D5c boundary at the wrong position", "(id, region)", ""),
            ("D5d ON DELETE SET NULL", "(region, i2)", " ON DELETE SET NULL"),
            ("D5e ON UPDATE SET DEFAULT", "(region, i2)", " ON UPDATE SET DEFAULT"),
        ],
    )
    async def test_c6_rejects_bad_foreign_keys_between_scoped_tables(
        self, guarded: Guarded, label: str, columns: str, action: str
    ) -> None:
        prelude = [
            _hatch("sites"),
            "CREATE TABLE sites.swapped (region integer NOT NULL, id serial, r2 integer NOT NULL, "
            "i2 integer NOT NULL, PRIMARY KEY (region, id))",
            _protect("sites", "sites.swapped", REGION, SELECT_ONLY),
        ]
        fk = (
            f"ALTER TABLE sites.swapped ADD FOREIGN KEY {columns} "
            f"REFERENCES sites.site_readings (region, id){action}"
        )
        assert await _run(guarded, role="sites_owner", prelude=prelude, statement=fk) == "LG002", (
            label
        )

    @pytest.mark.parametrize(
        ("label", "prelude", "want"),
        [
            (
                "D6a same qual, different cmd",
                [
                    "DROP POLICY loom_select ON notes.notes",
                    _policy("loom_select", "ALL", "PUBLIC", f"USING ({QUAL})"),
                ],
                "LG002",
            ),
            (
                "D6b owner trigger with a no-op function",
                [
                    "CREATE FUNCTION notes.noop() RETURNS trigger LANGUAGE plpgsql AS "
                    "$$ BEGIN RETURN NULL; END $$",
                    *_trigger(ALL_EVENTS, "notes.noop()"),
                ],
                "LG002",
            ),
            (
                "D6c a rule on a scoped table",
                [
                    "CREATE RULE copy_all AS ON INSERT TO notes.notes DO ALSO "
                    "INSERT INTO notes.note_kinds (name) VALUES (NEW.body)"
                ],
                "LG002",
            ),
            ("D6h trigger for DELETE only", _trigger("DELETE", DENY), "LG002"),
            (
                "D6i trigger with a WHEN clause",
                _trigger(ALL_EVENTS, DENY, "WHEN (false) "),
                "LG002",
            ),
            (
                "D6m trigger with UPDATE OF a column",
                _trigger("INSERT OR UPDATE OF body OR DELETE OR TRUNCATE", DENY),
                "LG002",
            ),
            (
                "D6n positive control: recreated exactly as generated",
                [
                    "DROP POLICY loom_select ON notes.notes",
                    _policy("loom_select", "SELECT", "PUBLIC", f"USING ({QUAL})"),
                ],
                "none",
            ),
            (
                "D6j narrower role list",
                [
                    "DROP POLICY loom_select ON notes.notes",
                    _policy("loom_select", "SELECT", "notes_readers", f"USING ({QUAL})"),
                ],
                "LG002",
            ),
            (
                "D6k weaker with_check",
                [
                    "DROP POLICY loom_insert ON notes.notes",
                    _policy("loom_insert", "INSERT", "PUBLIC", "WITH CHECK (true)"),
                ],
                "LG002",
            ),
            (
                "D6l RESTRICTIVE instead of PERMISSIVE",
                [
                    "DROP POLICY loom_select ON notes.notes",
                    _policy("loom_select", "SELECT", "PUBLIC", f"USING ({QUAL})", restrictive=True),
                ],
                "LG002",
            ),
        ],
    )
    async def test_d6_the_assertion_is_complete_over_what_protect_creates(
        self, guarded: Guarded, label: str, prelude: list[str], want: str
    ) -> None:
        got = await _run(
            guarded, role="notes_owner", prelude=[_hatch("notes"), *prelude], statement=ASSERT_NOTES
        )
        assert got == want, label

    @pytest.mark.parametrize(
        ("label", "statement"),
        [
            (
                "D6d ALTER POLICY weakening the text",
                "ALTER POLICY loom_select ON notes.notes USING (true)",
            ),
            (
                "D6e extra permissive policy",
                "CREATE POLICY sneaky ON notes.notes FOR SELECT TO PUBLIC USING (true)",
            ),
            (
                "D6f UNIQUE INDEX without the boundary",
                "CREATE UNIQUE INDEX sneaky_idx ON notes.notes (editor)",
            ),
            ("D6g NO FORCE RLS", "ALTER TABLE notes.notes NO FORCE ROW LEVEL SECURITY"),
        ],
    )
    async def test_d6_the_event_trigger_catches_manipulation_without_the_hatch(
        self, guarded: Guarded, label: str, statement: str
    ) -> None:
        assert await _run(guarded, role="notes_owner", statement=statement) == "LG002", label


class TestUnprotect:
    async def test_d7_unprotect_semantics(self, guarded: Guarded) -> None:
        create = [
            _hatch("notes"),
            _scoped("notes.tmp_scoped"),
        ]
        protect = _protect("notes", "notes.tmp_scoped", OWNER, RW)
        unprotect = "SELECT loom_guard_notes.unprotect_scoped_table('notes.tmp_scoped')"
        assert (
            await _run(guarded, role="notes_owner", prelude=create, statement=protect, commit=True)
            == "none"
        )
        async with guarded.connection() as conn:
            await conn.execute(f"INSERT INTO notes.tmp_scoped (owner_id) VALUES ('{U1}')")
        assert (
            await _run(guarded, role="notes_owner", statement="DROP TABLE notes.tmp_scoped")
            == "LG002"
        )
        hatch = [_hatch("notes")]
        assert (
            await _run(guarded, role="notes_owner", prelude=hatch, statement=unprotect, commit=True)
            == "none"
        )
        check = f"{_count('notes.tmp_scoped')} = 0"
        assert await _truth(guarded, role="notes_rw", prelude=[_owner_key(U1)], check=check)
        drop = ["DROP TABLE notes.tmp_scoped"]
        assert (
            await _run(
                guarded, role="notes_owner", prelude=drop, statement=ASSERT_NOTES, commit=True
            )
            == "none"
        )
        unknown = "SELECT loom_guard_notes.unprotect_scoped_table('notes.note_kinds')"
        assert await _run(guarded, role="notes_owner", prelude=hatch, statement=unknown) == "42501"


class TestIncrementalDdlAndRepair:
    CHILD = (
        "CREATE TABLE notes.child_late (owner_id uuid NOT NULL, id serial, note_id int NOT NULL, "
        "PRIMARY KEY (owner_id, id), FOREIGN KEY (owner_id, note_id) "
        "REFERENCES notes.notes (owner_id, id) "
        "ON DELETE CASCADE)"
    )
    PARTITION = (
        "CREATE TABLE notes.note_events_2027 PARTITION OF notes.note_events "
        "FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')"
    )

    async def test_d8a_d8b_a_new_child_needs_the_hatch_and_passes_with_it(
        self, guarded: Guarded
    ) -> None:
        assert await _run(guarded, role="notes_owner", statement=self.CHILD) == "LG002"
        protect = _protect("notes", "notes.child_late", OWNER, RW)
        prelude = [_hatch("notes"), self.CHILD]
        assert (
            await _run(guarded, role="notes_owner", prelude=prelude, statement=protect, commit=True)
            == "none"
        )
        assert await _run(guarded, statement=ASSERT_NOTES) == "none"

    async def test_d8c_d8d_a_new_partition_needs_the_hatch_and_passes_with_it(
        self, guarded: Guarded
    ) -> None:
        assert await _run(guarded, role="notes_owner", statement=self.PARTITION) == "LG002"
        protect = _protect("notes", "notes.note_events_2027", OWNER, SELECT_ONLY)
        prelude = [_hatch("notes"), self.PARTITION]
        assert (
            await _run(guarded, role="notes_owner", prelude=prelude, statement=protect, commit=True)
            == "none"
        )
        assert await _run(guarded, statement=ASSERT_NOTES) == "none"

    async def test_d8e_d8f_a_drifted_policy_is_repaired_under_the_hatch(
        self, guarded: Guarded
    ) -> None:
        drift = "ALTER POLICY loom_select ON notes.child_late USING (true)"
        assert (
            await _run(
                guarded, role="notes_owner", prelude=[_hatch("notes")], statement=drift, commit=True
            )
            == "none"
        )
        assert await _run(guarded, statement=ASSERT_NOTES) == "LG002"
        protect = _protect("notes", "notes.child_late", OWNER, RW)
        unprotect = "SELECT loom_guard_notes.unprotect_scoped_table('notes.child_late')"
        together = f"{unprotect}; {protect}"
        assert await _run(guarded, role="notes_owner", statement=together) == "LG002"
        prelude = [_hatch("notes"), unprotect]
        assert (
            await _run(guarded, role="notes_owner", prelude=prelude, statement=protect, commit=True)
            == "none"
        )
        assert await _run(guarded, statement=ASSERT_NOTES) == "none"


class TestPreconditionsAndSchemaPredicates:
    async def test_p1_protect_refuses_a_table_outside_its_schema(self, guarded: Guarded) -> None:
        protect = _protect("notes", "sites.site_archive", REGION, SELECT_ONLY)
        assert await _run(guarded, statement=protect) == "42501"

    async def test_p2_protect_refuses_a_table_that_already_has_policies(
        self, guarded: Guarded
    ) -> None:
        protect = _protect("notes", "notes.notes", OWNER, SELECT_ONLY)
        assert await _run(guarded, role="notes_owner", statement=protect) == "42501"

    async def test_p6_protect_refuses_a_nullable_boundary(self, guarded: Guarded) -> None:
        prelude = [
            _hatch("notes"),
            "CREATE TABLE notes.nullable_b (owner_id uuid, id serial PRIMARY KEY)",
        ]
        protect = _protect("notes", "notes.nullable_b", OWNER, SELECT_ONLY)
        assert (
            await _run(guarded, role="notes_owner", prelude=prelude, statement=protect) == "22023"
        )

    async def test_p7_protect_refuses_a_pk_without_the_boundary_and_leaves_nothing_behind(
        self, guarded: Guarded
    ) -> None:
        protect = _protect("notes", "notes.pk_no_boundary", OWNER, SELECT_ONLY)
        policies = (
            "SELECT count(*) FROM pg_policy WHERE polrelid = 'notes.pk_no_boundary'::regclass"
        )
        registered = (
            "SELECT EXISTS (SELECT 1 FROM loom_guard_notes.scoped_table "
            "WHERE rel = 'notes.pk_no_boundary'::regclass)"
        )
        async with guarded.connection() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                await conn.execute(_hatch("notes"))
                await conn.execute(_pk_only("notes.pk_no_boundary"))
                await conn.execute("ALTER TABLE notes.pk_no_boundary OWNER TO notes_owner")
                assert await _attempt_as(conn, "notes_owner", protect) == "LG002"
                assert await conn.fetchval(policies) == 0
                assert not await conn.fetchval(registered)
            finally:
                await tx.rollback()

    @pytest.mark.parametrize(
        ("label", "statement"),
        [
            (
                "P8 owned by someone else",
                "CREATE TABLE notes.foreign_owned (x int); "
                "ALTER TABLE notes.foreign_owned OWNER TO sites_owner",
            ),
            ("P8b owned by the superuser", "CREATE TABLE notes.super_owned (x int)"),
        ],
    )
    async def test_p8_ownership_is_asserted_for_every_relation_of_the_schema(
        self, guarded: Guarded, label: str, statement: str
    ) -> None:
        assert await _run(guarded, statement=statement) == "LG002", label

    async def test_p9_a_materialized_view_is_out_of_scope(self, guarded: Guarded) -> None:
        view = "CREATE MATERIALIZED VIEW notes.mv AS SELECT 1 AS x"
        assert await _run(guarded, role="notes_owner", statement=view) == "LG002"

    async def test_p10_protect_refuses_a_table_not_owned_by_the_owner_role(
        self, guarded: Guarded
    ) -> None:
        prelude = [
            _hatch("notes"),
            _scoped("notes.not_mine"),
            "ALTER TABLE notes.not_mine OWNER TO sites_owner",
            "SET LOCAL ROLE notes_owner",
        ]
        protect = _protect("notes", "notes.not_mine", OWNER, SELECT_ONLY)
        assert await _run(guarded, prelude=prelude, statement=protect) == "42501"


class TestThreeProductsInOneDatabase:
    async def test_x1_x2_other_owners_and_temp_tables_work_without_guard_privileges(
        self, guarded: Guarded
    ) -> None:
        create = "CREATE TABLE sites.site_kinds (id serial PRIMARY KEY, name text)"
        assert await _run(guarded, role="sites_owner", statement=create) == "none"
        assert (
            await _run(guarded, role="notes_rw", statement="CREATE TEMP TABLE scratch (x int)")
            == "none"
        )

    async def test_x3_each_product_reads_its_own_boundary_type(self, guarded: Guarded) -> None:
        sites_keys = [
            "SELECT set_config('loom.scope.region', '1', true), "
            f"set_config('loom.scope.owner', '{U2}', true), "
            "set_config('loom.scope.account', '200', true)"
        ]
        readings = (
            "(SELECT sum(value) FROM sites.site_readings) = 10 "
            "AND (SELECT count(*) FROM sites.site_readings) = 1"
        )
        assert await _truth(guarded, role="sites_rw", prelude=sites_keys, check=readings)
        ledger_keys = ["SELECT set_config('loom.scope.account', '100', true)"]
        entries = (
            "(SELECT sum(amount) FROM ledger.entries) = 1 "
            "AND (SELECT count(*) FROM ledger.accounts) = 2"
        )
        assert await _truth(guarded, role="ledger_rw", prelude=ledger_keys, check=entries)
        cross = "INSERT INTO ledger.entries (account_id, amount) VALUES (200, 9)"
        assert (
            await _run(guarded, role="ledger_rw", prelude=ledger_keys, statement=cross) == "42501"
        )
        parent = "INSERT INTO ledger.accounts VALUES (300,'x')"
        assert (
            await _run(guarded, role="ledger_rw", prelude=ledger_keys, statement=parent) == "42501"
        )

    async def test_x4_to_x7_a_broken_guard_blocks_ddl_everywhere_until_repaired(
        self, guarded: Guarded
    ) -> None:
        break_rls = "ALTER TABLE sites.site_readings NO FORCE ROW LEVEL SECURITY"
        repair = "ALTER TABLE sites.site_readings FORCE ROW LEVEL SECURITY"
        hatch = [_hatch("sites")]
        assert (
            await _run(guarded, role="sites_owner", prelude=hatch, statement=break_rls, commit=True)
            == "none"
        )
        try:
            assert (
                await _run(guarded, statement="SELECT loom_guard_sites.assert_scoped_schema()")
                == "LG002"
            )
            scratch = _scoped("notes.scratch_a")
            assert await _run(guarded, role="notes_owner", statement=scratch) == "LG002"
            assert (
                await _run(guarded, role="notes_rw", statement="CREATE TEMP TABLE scratch2 (x int)")
                == "LG002"
            )
        finally:
            assert (
                await _run(
                    guarded, role="sites_owner", prelude=hatch, statement=repair, commit=True
                )
                == "none"
            )
        assert (
            await _run(guarded, statement="SELECT loom_guard_sites.assert_scoped_schema()")
            == "none"
        )

    async def test_x5_the_blocking_error_names_the_broken_schema(self, guarded: Guarded) -> None:
        async with guarded.connection() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL ROLE sites_owner")
                await conn.execute(_hatch("sites"))
                await conn.execute("ALTER TABLE sites.site_readings NO FORCE ROW LEVEL SECURITY")
            try:
                with pytest.raises(asyncpg.PostgresError) as failure:
                    await conn.execute("CREATE TEMP TABLE scratch3 (x int)")
                assert failure.value.sqlstate == "LG002"
                assert "loom_guard[sites]" in str(failure.value)
            finally:
                async with conn.transaction():
                    await conn.execute("SET LOCAL ROLE sites_owner")
                    await conn.execute(_hatch("sites"))
                    await conn.execute("ALTER TABLE sites.site_readings FORCE ROW LEVEL SECURITY")


class TestMembershipsAndLaterUsers:
    async def test_v10_a_non_bypass_member_of_a_bypass_role_is_caught(
        self, guarded: Guarded
    ) -> None:
        async with guarded.connection() as conn:
            await conn.execute("GRANT notes_ops TO notes_rw")
            try:
                assert await _sqlstate(conn, ASSERT_NOTES) == "LG002"
            finally:
                await conn.execute("REVOKE notes_ops FROM notes_rw")

    async def test_m7_1_a_user_added_later_reads_its_boundary_with_no_ddl(
        self, guarded: Guarded
    ) -> None:
        async with guarded.connection() as conn:
            await conn.execute("DROP ROLE IF EXISTS notes_new")
            await conn.execute("CREATE ROLE notes_new NOLOGIN NOBYPASSRLS INHERIT")
            await conn.execute("GRANT notes_readers TO notes_new")
        assert await _truth(
            guarded,
            role="notes_new",
            prelude=[_owner_key(U1)],
            check=f"{_count('notes.notes')} = 2",
        )

    async def test_b1_the_bypass_user_sees_every_row_without_keys(self, guarded: Guarded) -> None:
        assert await _truth(guarded, role="notes_ops", check=f"{_count('notes.notes')} = 3")

    async def test_z_final_all_three_assertions_are_green(self, guarded: Guarded) -> None:
        assert await _run(guarded, statement=_all_assertions()) == "none"
