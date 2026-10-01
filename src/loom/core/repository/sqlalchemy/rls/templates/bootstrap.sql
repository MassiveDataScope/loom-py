-- Bootstrap of one application schema, rendered by loom from the product's declaration. Every name below was
-- injected, none is loom's. Idempotent: re-applying after adding a database user is the supported way to grant it.
-- SQLSTATEs: LG001 owner DML denied, LG002 guard invariant violated, 22023 bad declaration, 42501 not the owner
-- or not privileged.
DO $$
BEGIN
  IF current_setting('server_version_num')::int < {MIN_SERVER_VERSION_NUM} THEN
    RAISE EXCEPTION 'loom_guard[{S}]: server_version_num % is below {MIN_SERVER_VERSION_NUM}', current_setting('server_version_num') USING ERRCODE = '22023';
  END IF;
  IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AND NOT pg_has_role(current_user, 'rds_superuser', 'MEMBER') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: creating an event trigger needs superuser or rds_superuser; current_user is %', current_user USING ERRCODE = '42501';
  END IF;
END $$;
-- roles and users: compare attributes when they exist, create when they do not
DO $$
DECLARE spec record; r pg_roles%ROWTYPE;
BEGIN
  FOR spec IN SELECT * FROM (VALUES {ROLE_ROWS}) AS v(name_, login_, bypass_, inherit_) LOOP
    SELECT * INTO r FROM pg_roles WHERE rolname = spec.name_;
    IF NOT FOUND THEN
      EXECUTE format('CREATE ROLE %I %s %s %s', spec.name_,
        CASE WHEN spec.login_ THEN 'LOGIN' ELSE 'NOLOGIN' END,
        CASE WHEN spec.bypass_ THEN 'BYPASSRLS' ELSE 'NOBYPASSRLS' END,
        CASE WHEN spec.inherit_ THEN 'INHERIT' ELSE 'NOINHERIT' END);
    ELSIF r.rolcanlogin <> spec.login_ OR r.rolbypassrls <> spec.bypass_ OR r.rolinherit <> spec.inherit_ OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb THEN
      RAISE EXCEPTION 'loom_guard[{S}]: role % already exists with different attributes', spec.name_ USING ERRCODE = '42501';
    END IF;
  END LOOP;
END $$;
GRANT {OWNER} TO {MIGRATOR};
ALTER ROLE {MIGRATOR} SET role = {OWNER};
ALTER ROLE {OWNER} SET search_path = {S};
ALTER ROLE {MIGRATOR} SET search_path = {S};
{USER_STATEMENTS}
CREATE SCHEMA IF NOT EXISTS {S} AUTHORIZATION {OWNER};
{REVOKE_PUBLIC}
REVOKE CREATE ON SCHEMA {S} FROM PUBLIC;
GRANT USAGE ON SCHEMA {S} TO {READERS}, {WRITERS};
{BYPASS_STATEMENTS}
-- guard objects for this schema
CREATE SCHEMA IF NOT EXISTS loom_guard_{S};
CREATE TABLE IF NOT EXISTS loom_guard_{S}.scoped_table (rel regclass PRIMARY KEY, scopes jsonb NOT NULL, privileges text[] NOT NULL);
CREATE TABLE IF NOT EXISTS loom_guard_{S}.scoped_policy (rel regclass, policyname name, cmd text, roles name[], qual text, with_check text, PRIMARY KEY (rel, policyname));
REVOKE ALL ON SCHEMA loom_guard_{S} FROM PUBLIC;
GRANT USAGE ON SCHEMA loom_guard_{S} TO {OWNER};
GRANT SELECT ON loom_guard_{S}.scoped_table, loom_guard_{S}.scoped_policy TO {OWNER};
CREATE OR REPLACE FUNCTION loom_guard_{S}.term(col name, scope text, elevable boolean, coltype text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN elevable
    THEN format('(%I = NULLIF(current_setting(%L, true), %L)::%s OR current_setting(%L, true) = %L)', col, 'loom.scope.'||scope, '', coltype, 'loom.scope.'||scope||'.any', 'on')
    ELSE format('%I = NULLIF(current_setting(%L, true), %L)::%s', col, 'loom.scope.'||scope, '', coltype) END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.deny_owner_dml() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
  IF (SELECT relowner FROM pg_class WHERE oid = TG_RELID) = (SELECT oid FROM pg_roles WHERE rolname = current_user)
  THEN RAISE EXCEPTION 'loom_guard[%]: owner DML on scoped table %.% is denied; run data migrations as the bypass user', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_NAME USING ERRCODE = 'LG001'; END IF;
  RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.protect_scoped_table(tbl regclass, scopes jsonb, table_privileges text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE sch name; relname_ name; r text; w text; e jsonb; seq record; writes text[]; boundary_count int;
BEGIN
  SELECT n.nspname, c.relname INTO sch, relname_ FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = tbl;
  IF sch <> '{S}' THEN RAISE EXCEPTION 'loom_guard[{S}]: % is not in schema {S}', tbl USING ERRCODE = '42501'; END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT relowner FROM pg_class WHERE oid = tbl)) <> '{OWNER}'
     OR NOT pg_has_role(session_user, '{OWNER}', 'MEMBER') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: only the owner may protect %', tbl USING ERRCODE = '42501'; END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl) THEN
    RAISE EXCEPTION 'loom_guard[{S}]: % already has policies; protect never adopts existing ones', tbl USING ERRCODE = '42501'; END IF;
  IF NOT (table_privileges <@ ARRAY['SELECT','INSERT','UPDATE','DELETE']) THEN RAISE EXCEPTION 'loom_guard[{S}]: privilege outside the closed set' USING ERRCODE = '22023'; END IF;
  IF jsonb_typeof(scopes) <> 'array' OR jsonb_array_length(scopes) = 0 THEN RAISE EXCEPTION 'loom_guard[{S}]: scopes must be a non-empty array' USING ERRCODE = '22023'; END IF;
  FOR e IN SELECT * FROM jsonb_array_elements(scopes) LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = e->>'col' AND attnum > 0 AND NOT attisdropped) THEN
      RAISE EXCEPTION 'loom_guard[{S}]: scope column % does not exist on %', e->>'col', tbl USING ERRCODE = '22023'; END IF;
    IF coalesce(e->>'on', 'both') NOT IN ('read','write','both') THEN
      RAISE EXCEPTION 'loom_guard[{S}]: scope reach % is not read/write/both', e->>'on' USING ERRCODE = '22023'; END IF;
    IF (e->>'scope') IS NULL OR (e->>'scope') !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
      RAISE EXCEPTION 'loom_guard[{S}]: scope name % is not an identifier', e->>'scope' USING ERRCODE = '22023'; END IF;
  END LOOP;
  SELECT count(*) INTO boundary_count FROM jsonb_array_elements(scopes) x WHERE coalesce(x->>'on','both') = 'both' AND NOT coalesce((x->>'elevable')::boolean, false);
  IF boundary_count <> 1 THEN RAISE EXCEPTION 'loom_guard[{S}]: a scoped table needs exactly one non-elevable boundary scope (got %)', boundary_count USING ERRCODE = '22023'; END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x WHERE coalesce((x->>'elevable')::boolean,false) AND coalesce(x->>'on','both') <> 'write') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: elevable scopes must be write-only' USING ERRCODE = '22023'; END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col' WHERE coalesce(x->>'on','both') = 'both' AND NOT a.attnotnull) THEN
    RAISE EXCEPTION 'loom_guard[{S}]: boundary column must be NOT NULL' USING ERRCODE = '22023'; END IF;
  IF EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = tbl AND (i.indisunique OR i.indisexclusion) AND EXISTS (
       SELECT 1 FROM jsonb_array_elements(scopes) x WHERE coalesce(x->>'on','both') = 'both' AND NOT coalesce((x->>'elevable')::boolean,false)
         AND (SELECT attnum FROM pg_attribute WHERE attrelid = tbl AND attname = x->>'col') <> ALL (i.indkey::int2[]))) THEN
    RAISE EXCEPTION 'loom_guard[{S}]: every unique index of % must contain the boundary column', tbl USING ERRCODE = 'LG002'; END IF;
  PERFORM set_config('loom_guard_{S}.protecting', 'on', true);
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
  SELECT string_agg(loom_guard_{S}.term((x->>'col')::name, x->>'scope', coalesce((x->>'elevable')::boolean,false), format_type(a.atttypid, a.atttypmod)), ' AND ') INTO r
    FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col' WHERE coalesce(x->>'on','both') IN ('read','both');
  SELECT string_agg(loom_guard_{S}.term((x->>'col')::name, x->>'scope', coalesce((x->>'elevable')::boolean,false), format_type(a.atttypid, a.atttypmod)), ' AND ') INTO w
    FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col' WHERE coalesce(x->>'on','both') IN ('write','both');
  EXECUTE format('CREATE POLICY loom_select ON %s FOR SELECT TO PUBLIC USING (%s)', tbl, r);
  IF 'INSERT' = ANY(table_privileges) THEN EXECUTE format('CREATE POLICY loom_insert ON %s FOR INSERT TO PUBLIC WITH CHECK (%s)', tbl, w); END IF;
  IF 'UPDATE' = ANY(table_privileges) THEN EXECUTE format('CREATE POLICY loom_update ON %s FOR UPDATE TO PUBLIC USING (%s) WITH CHECK (%s)', tbl, w, w); END IF;
  IF 'DELETE' = ANY(table_privileges) THEN EXECUTE format('CREATE POLICY loom_delete ON %s FOR DELETE TO PUBLIC USING (%s)', tbl, w); END IF;
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC, {READERS}, {WRITERS}', tbl);
  IF 'SELECT' = ANY(table_privileges) THEN EXECUTE format('GRANT SELECT ON %s TO {READERS}', tbl); END IF;
  SELECT array_agg(p) INTO writes FROM unnest(table_privileges) p WHERE p IN ('INSERT','UPDATE','DELETE');
  IF writes IS NOT NULL THEN
    EXECUTE format('GRANT %s ON %s TO {WRITERS}', array_to_string(writes, ','), tbl);
    IF 'INSERT' = ANY(writes) THEN
      FOR seq IN SELECT sq.relname FROM pg_depend d JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S' WHERE d.refobjid = tbl AND d.deptype = 'a' LOOP
        EXECUTE format('GRANT USAGE ON SEQUENCE %I.%I TO {WRITERS}', sch, seq.relname);
      END LOOP;
    END IF;
  END IF;
  EXECUTE format('CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION loom_guard_{S}.deny_owner_dml()', tbl);
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_deny_owner_dml', tbl);
  INSERT INTO loom_guard_{S}.scoped_table VALUES (tbl, scopes, table_privileges);
  INSERT INTO loom_guard_{S}.scoped_policy SELECT tbl, policyname, cmd, roles, qual, with_check FROM pg_policies p JOIN pg_class c ON c.relname = p.tablename JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname WHERE c.oid = tbl;
  PERFORM set_config('loom_guard_{S}.protecting', '', true);
END $$;
REVOKE ALL ON FUNCTION loom_guard_{S}.protect_scoped_table FROM PUBLIC;
GRANT EXECUTE ON FUNCTION loom_guard_{S}.protect_scoped_table TO {OWNER};
CREATE OR REPLACE FUNCTION loom_guard_{S}.unprotect_scoped_table(tbl regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE pol record;
BEGIN
  IF NOT pg_has_role(session_user, '{OWNER}', 'MEMBER') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: only the owner may unprotect %', tbl USING ERRCODE = '42501'; END IF;
  IF NOT EXISTS (SELECT 1 FROM loom_guard_{S}.scoped_table WHERE rel = tbl) THEN
    RAISE EXCEPTION 'loom_guard[{S}]: % is not registered', tbl USING ERRCODE = '42501'; END IF;
  DELETE FROM loom_guard_{S}.scoped_policy WHERE rel = tbl;
  DELETE FROM loom_guard_{S}.scoped_table WHERE rel = tbl;
  FOR pol IN SELECT * FROM (SELECT polname FROM pg_policy WHERE polrelid = tbl ORDER BY polname) q LOOP
    EXECUTE format('DROP POLICY %I ON %s', pol.polname, tbl);
  END LOOP;
  EXECUTE format('DROP TRIGGER IF EXISTS loom_deny_owner_dml ON %s', tbl);
END $$;
REVOKE ALL ON FUNCTION loom_guard_{S}.unprotect_scoped_table FROM PUBLIC;
GRANT EXECUTE ON FUNCTION loom_guard_{S}.unprotect_scoped_table TO {OWNER};
CREATE OR REPLACE FUNCTION loom_guard_{S}.assert_scoped_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE bad text;
BEGIN
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM loom_guard_{S}.scoped_table t JOIN pg_class c ON c.oid = t.rel WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: registered tables without forced RLS: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(DISTINCT t.rel::text, ', ') INTO bad FROM loom_guard_{S}.scoped_table t
    WHERE (SELECT count(*) FROM pg_policy p WHERE p.polrelid = t.rel) <> (SELECT count(*) FROM loom_guard_{S}.scoped_policy sp WHERE sp.rel = t.rel)
       OR EXISTS (SELECT 1 FROM loom_guard_{S}.scoped_policy sp
                  LEFT JOIN pg_policies p ON p.policyname = sp.policyname AND p.tablename = (SELECT relname FROM pg_class WHERE oid = t.rel) AND p.schemaname = '{S}'
                  WHERE sp.rel = t.rel AND (p.qual IS DISTINCT FROM sp.qual OR p.with_check IS DISTINCT FROM sp.with_check
                        OR p.roles IS DISTINCT FROM sp.roles OR p.cmd IS DISTINCT FROM sp.cmd OR p.permissive IS DISTINCT FROM 'PERMISSIVE'));
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: policy set differs from the registered canonical set: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM loom_guard_{S}.scoped_table t JOIN pg_class c ON c.oid = t.rel, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT coalesce(
          (g.rolname = '{READERS}' AND a.privilege_type = 'SELECT' AND 'SELECT' = ANY(t.privileges))
       OR (g.rolname = '{WRITERS}' AND a.privilege_type IN ('INSERT','UPDATE','DELETE') AND a.privilege_type = ANY(t.privileges))
       OR (g.rolname = ANY (ARRAY[{BYPASS_LIST}]::name[]) AND a.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE')), false);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: grant outside the whitelist: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(DISTINCT c.relname || '.' || at.attname, ', ') INTO bad FROM loom_guard_{S}.scoped_table t JOIN pg_class c ON c.oid = t.rel JOIN pg_attribute at ON at.attrelid = c.oid WHERE at.attacl IS NOT NULL;
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: column grants are not allowed: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(c.relname, ', ') INTO bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner
    WHERE n.nspname = '{S}' AND c.relkind IN ('r','p','v','m','S') AND (r.rolbypassrls OR r.rolsuper OR r.rolname <> '{OWNER}');
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: relations not owned by {OWNER}: %', bad USING ERRCODE = 'LG002'; END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{S}' AND c.relkind = 'm') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: materialized views are out of scope in a scoped schema' USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(DISTINCT rw.ev_class::regclass::text, ', ') INTO bad FROM pg_rewrite rw JOIN loom_guard_{S}.scoped_table t ON t.rel = rw.ev_class WHERE rw.rulename <> '_RETURN';
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: rules on scoped tables are not allowed: %', bad USING ERRCODE = 'LG002'; END IF;
  IF EXISTS (SELECT 1 FROM pg_roles m WHERE NOT m.rolbypassrls AND NOT m.rolsuper AND (pg_has_role(m.oid, '{READERS}', 'MEMBER') OR pg_has_role(m.oid, '{WRITERS}', 'MEMBER'))
             AND EXISTS (SELECT 1 FROM pg_roles b WHERE b.rolbypassrls AND pg_has_role(m.oid, b.oid, 'MEMBER')))
  THEN RAISE EXCEPTION 'loom_guard[{S}]: a non-bypass user is a member of a bypass role' USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM loom_guard_{S}.scoped_table t WHERE NOT EXISTS (
    SELECT 1 FROM pg_trigger g WHERE g.tgrelid = t.rel AND g.tgname = 'loom_deny_owner_dml' AND g.tgenabled = 'A'
      AND g.tgfoid = 'loom_guard_{S}.deny_owner_dml'::regproc AND (g.tgtype & 2) <> 0 AND (g.tgtype & 28) = 28 AND (g.tgtype & 32) <> 0 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: owner trigger missing, disabled, wrong function or wrong events: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO bad FROM pg_index i JOIN loom_guard_{S}.scoped_table t ON t.rel = i.indrelid
    WHERE (i.indisunique OR i.indisexclusion) AND EXISTS (
      SELECT 1 FROM jsonb_array_elements(t.scopes) x WHERE coalesce(x->>'on','both') = 'both' AND NOT coalesce((x->>'elevable')::boolean,false)
        AND (SELECT attnum FROM pg_attribute WHERE attrelid = t.rel AND attname = x->>'col') <> ALL (i.indkey::int2[]));
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: unique index without the boundary column: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con
    JOIN loom_guard_{S}.scoped_table t ON t.rel = con.conrelid JOIN loom_guard_{S}.scoped_table t2 ON t2.rel = con.confrelid
    WHERE con.contype = 'f' AND (
      con.confdeltype IN ('n','d') OR con.confupdtype IN ('n','d')
      OR NOT EXISTS (SELECT 1 FROM generate_subscripts(con.conkey, 1) k
                     WHERE con.conkey[k] = (SELECT attnum FROM pg_attribute WHERE attrelid = t.rel AND attname = (SELECT x->>'col' FROM jsonb_array_elements(t.scopes) x WHERE coalesce(x->>'on','both')='both' AND NOT coalesce((x->>'elevable')::boolean,false)))
                       AND con.confkey[k] = (SELECT attnum FROM pg_attribute WHERE attrelid = t2.rel AND attname = (SELECT x->>'col' FROM jsonb_array_elements(t2.scopes) x WHERE coalesce(x->>'on','both')='both' AND NOT coalesce((x->>'elevable')::boolean,false)))));
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: FK between scoped tables must map boundary to boundary and never SET NULL/SET DEFAULT: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f' AND n.nspname = '{S}' AND con.confrelid IN (SELECT rel FROM loom_guard_{S}.scoped_table) AND con.conrelid NOT IN (SELECT rel FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: FK from an unscoped table into a scoped one: %', bad USING ERRCODE = 'LG002'; END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i JOIN loom_guard_{S}.scoped_table t ON t.rel = i.inhparent WHERE i.inhrelid NOT IN (SELECT rel FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: unregistered partitions: %', bad USING ERRCODE = 'LG002'; END IF;
END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.on_ddl_end() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF current_setting('loom_guard_{S}.protecting', true) = 'on' THEN RETURN; END IF;
  PERFORM loom_guard_{S}.assert_scoped_schema();
END $$;
DROP EVENT TRIGGER IF EXISTS loom_guard_{S}_ddl;
CREATE EVENT TRIGGER loom_guard_{S}_ddl ON ddl_command_end EXECUTE FUNCTION loom_guard_{S}.on_ddl_end();
CREATE OR REPLACE FUNCTION loom_guard_{S}.on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE bad text;
BEGIN
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'loom_guard[{S}]: unprotect before dropping a registered table: %', bad USING ERRCODE = 'LG002'; END IF;
END $$;
DROP EVENT TRIGGER IF EXISTS loom_guard_{S}_drop;
CREATE EVENT TRIGGER loom_guard_{S}_drop ON sql_drop EXECUTE FUNCTION loom_guard_{S}.on_sql_drop();
