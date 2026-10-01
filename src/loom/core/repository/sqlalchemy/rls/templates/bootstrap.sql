DO $$
BEGIN
  IF current_setting('server_version_num')::int < {MIN_SERVER_VERSION_NUM} THEN
    RAISE EXCEPTION 'loom_guard[{S}]: server_version_num % is below {MIN_SERVER_VERSION_NUM}', current_setting('server_version_num') USING ERRCODE = '22023';
  END IF;
  IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AND NOT pg_has_role(current_user, 'rds_superuser', 'MEMBER') THEN
    RAISE EXCEPTION 'loom_guard[{S}]: creating an event trigger needs superuser or rds_superuser; current_user is %', current_user USING ERRCODE = '42501';
  END IF;
END $$;
CREATE SCHEMA IF NOT EXISTS loom_guard_{S};
CREATE OR REPLACE FUNCTION loom_guard_{S}.fail(code text, message text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'loom_guard[{S}]: %', message USING ERRCODE = code;
END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.reject(message text) RETURNS void LANGUAGE sql AS $$
  SELECT loom_guard_{S}.fail('22023', message) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.refuse(message text) RETURNS void LANGUAGE sql AS $$
  SELECT loom_guard_{S}.fail('42501', message) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.violation(message text) RETURNS void LANGUAGE sql AS $$
  SELECT loom_guard_{S}.fail('LG002', message) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.is_member(member name, role name) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT pg_has_role(member, role, 'MEMBER') $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.owner_role() RETURNS name LANGUAGE sql IMMUTABLE AS $$
  SELECT '{OWNER}'::name $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.group_role(is_write boolean) RETURNS name LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN is_write THEN '{WRITERS}'::name ELSE '{READERS}'::name END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.commands()
  RETURNS TABLE (privilege text, is_write boolean, uses_using boolean, uses_check boolean, needs_sequences boolean)
  LANGUAGE sql IMMUTABLE AS $$
  VALUES ('SELECT', false, true, false, false),
         ('INSERT', true, false, true, true),
         ('UPDATE', true, true, true, false),
         ('DELETE', true, true, false, false) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.reaches() RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['read', 'write', 'both'] $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.reach(e jsonb) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(e->>'on', 'both') $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.elevable(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce((e->>'elevable')::boolean, false) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.applies(e jsonb, for_reads boolean) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT loom_guard_{S}.reach(e) <> CASE WHEN for_reads THEN 'write' ELSE 'read' END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.is_boundary(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT loom_guard_{S}.applies(e, true) AND loom_guard_{S}.applies(e, false) AND NOT loom_guard_{S}.elevable(e) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.boundary_attnum(rel regclass, scopes jsonb) RETURNS int2 LANGUAGE sql STABLE AS $$
  SELECT a.attnum FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = rel AND a.attname = x->>'col'
  WHERE loom_guard_{S}.is_boundary(x) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.set_hatch(state text) RETURNS void LANGUAGE sql AS $$
  SELECT set_config('loom_guard_{S}.protecting', state, true) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.hatch_open() RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT coalesce(current_setting('loom_guard_{S}.protecting', true) = 'on', false) $$;
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
      PERFORM loom_guard_{S}.refuse(format('role %s already exists with different attributes', spec.name_));
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
CREATE TABLE IF NOT EXISTS loom_guard_{S}.scoped_table (rel regclass PRIMARY KEY, scopes jsonb NOT NULL, privileges text[] NOT NULL);
CREATE TABLE IF NOT EXISTS loom_guard_{S}.scoped_policy (rel regclass, policyname name, cmd text, roles name[], qual text, with_check text, PRIMARY KEY (rel, policyname));
REVOKE ALL ON SCHEMA loom_guard_{S} FROM PUBLIC;
GRANT USAGE ON SCHEMA loom_guard_{S} TO {OWNER};
GRANT SELECT ON loom_guard_{S}.scoped_table, loom_guard_{S}.scoped_policy TO {OWNER};
CREATE OR REPLACE FUNCTION loom_guard_{S}.term(col name, scope text, elevable boolean, coltype text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN elevable THEN format('(%s OR current_setting(%L, true) = %L)', e.equality, k.setting_name || '.any', 'on') ELSE e.equality END
  FROM (SELECT 'loom.scope.' || scope AS setting_name) k,
       LATERAL (SELECT format('%I = NULLIF(current_setting(%L, true), %L)::%s', col, k.setting_name, '', coltype) AS equality) e $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.predicate(tbl regclass, scopes jsonb, for_reads boolean) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(loom_guard_{S}.term((x->>'col')::name, x->>'scope', loom_guard_{S}.elevable(x), format_type(a.atttypid, NULL)), ' AND ')
  FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
  WHERE loom_guard_{S}.applies(x, for_reads) $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.deny_owner_dml() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
  IF pg_has_role(current_user, (SELECT relowner FROM pg_catalog.pg_class WHERE oid = TG_RELID), 'USAGE')
     AND NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user)
  THEN RAISE EXCEPTION 'loom_guard[%]: owner DML on scoped table %.% is denied; run data migrations as the bypass user', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_NAME USING ERRCODE = 'LG001'; END IF;
  RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.protect_scoped_table(tbl regclass, scopes jsonb, table_privileges text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE sch name; e jsonb; scope_name text; command record; seq record; boundary_count int;
BEGIN
  SELECT n.nspname INTO sch FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = tbl;
  IF sch <> '{S}' THEN PERFORM loom_guard_{S}.refuse(format('%s is not in schema {S}', tbl)); END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT relowner FROM pg_class WHERE oid = tbl)) <> loom_guard_{S}.owner_role()
     OR NOT loom_guard_{S}.is_member(session_user, loom_guard_{S}.owner_role()) THEN
    PERFORM loom_guard_{S}.refuse(format('only the owner may protect %s', tbl)); END IF;
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl) THEN
    PERFORM loom_guard_{S}.refuse(format('%s already has policies; protect never adopts existing ones', tbl)); END IF;
  IF NOT (table_privileges <@ ARRAY(SELECT privilege FROM loom_guard_{S}.commands())) THEN
    PERFORM loom_guard_{S}.reject('privilege outside the closed set'); END IF;
  IF jsonb_typeof(scopes) <> 'array' OR jsonb_array_length(scopes) = 0 THEN
    PERFORM loom_guard_{S}.reject('scopes must be a non-empty array'); END IF;
  FOR e IN SELECT * FROM jsonb_array_elements(scopes) LOOP
    scope_name := e->>'scope';
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = e->>'col' AND attnum > 0 AND NOT attisdropped) THEN
      PERFORM loom_guard_{S}.reject(format('scope column %s does not exist on %s', e->>'col', tbl)); END IF;
    IF NOT loom_guard_{S}.reach(e) = ANY (loom_guard_{S}.reaches()) THEN
      PERFORM loom_guard_{S}.reject(format('scope reach %s is not read/write/both', e->>'on')); END IF;
    IF coalesce(scope_name, '') !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
      PERFORM loom_guard_{S}.reject(format('scope name %s is not an identifier', scope_name)); END IF;
  END LOOP;
  SELECT count(*) INTO boundary_count FROM jsonb_array_elements(scopes) x WHERE loom_guard_{S}.is_boundary(x);
  IF boundary_count <> 1 THEN
    PERFORM loom_guard_{S}.reject(format('a scoped table needs exactly one non-elevable boundary scope (got %s)', boundary_count)); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x WHERE loom_guard_{S}.elevable(x) AND loom_guard_{S}.applies(x, true)) THEN
    PERFORM loom_guard_{S}.reject('elevable scopes must be write-only'); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
             WHERE loom_guard_{S}.is_boundary(x) AND NOT a.attnotnull) THEN
    PERFORM loom_guard_{S}.reject('boundary column must be NOT NULL'); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
             LEFT JOIN pg_collation co ON co.oid = a.attcollation
             WHERE a.atttypid = 'bpchar'::regtype OR NOT coalesce(co.collisdeterministic, true)) THEN
    PERFORM loom_guard_{S}.reject('scope columns may not be char(n) or use a nondeterministic collation: equal values must be identical'); END IF;
  IF EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = tbl AND (i.indisunique OR i.indisexclusion)
             AND loom_guard_{S}.boundary_attnum(tbl, scopes) <> ALL ((i.indkey::int2[])[0:i.indnkeyatts - 1])) THEN
    PERFORM loom_guard_{S}.violation(format('every unique index of %s must contain the boundary column', tbl)); END IF;
  PERFORM loom_guard_{S}.set_hatch('on');
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
  EXECUTE format('CREATE POLICY loom_select ON %s FOR SELECT TO PUBLIC USING (%s)', tbl, loom_guard_{S}.predicate(tbl, scopes, true));
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC, {READERS}, {WRITERS}', tbl);
  FOR command IN SELECT * FROM loom_guard_{S}.commands() c WHERE c.privilege = ANY (table_privileges) LOOP
    IF command.is_write THEN
      EXECUTE format('CREATE POLICY %I ON %s FOR %s TO PUBLIC%s%s', 'loom_' || lower(command.privilege), tbl, command.privilege,
        CASE WHEN command.uses_using THEN format(' USING (%s)', loom_guard_{S}.predicate(tbl, scopes, false)) ELSE '' END,
        CASE WHEN command.uses_check THEN format(' WITH CHECK (%s)', loom_guard_{S}.predicate(tbl, scopes, false)) ELSE '' END);
    END IF;
    EXECUTE format('GRANT %s ON %s TO %I', command.privilege, tbl, loom_guard_{S}.group_role(command.is_write));
    IF command.needs_sequences THEN
      FOR seq IN SELECT sq.relname FROM pg_depend d JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S' WHERE d.refobjid = tbl AND d.deptype = 'a' LOOP
        EXECUTE format('GRANT USAGE ON SEQUENCE %I.%I TO {WRITERS}', sch, seq.relname);
      END LOOP;
    END IF;
  END LOOP;
  EXECUTE format('CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION loom_guard_{S}.deny_owner_dml()', tbl);
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_deny_owner_dml', tbl);
  INSERT INTO loom_guard_{S}.scoped_table VALUES (tbl, scopes, table_privileges);
  INSERT INTO loom_guard_{S}.scoped_policy SELECT tbl, policyname, cmd, roles, qual, with_check FROM pg_policies p JOIN pg_class c ON c.relname = p.tablename JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname WHERE c.oid = tbl;
  PERFORM loom_guard_{S}.set_hatch('');
END $$;
REVOKE ALL ON FUNCTION loom_guard_{S}.protect_scoped_table FROM PUBLIC;
GRANT EXECUTE ON FUNCTION loom_guard_{S}.protect_scoped_table TO {OWNER};
CREATE OR REPLACE FUNCTION loom_guard_{S}.unprotect_scoped_table(tbl regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE pol record;
BEGIN
  IF NOT loom_guard_{S}.is_member(session_user, loom_guard_{S}.owner_role()) THEN
    PERFORM loom_guard_{S}.refuse(format('only the owner may unprotect %s', tbl)); END IF;
  IF NOT loom_guard_{S}.hatch_open() THEN
    PERFORM loom_guard_{S}.violation(format('unprotect %s only under the hatch, in the transaction that changes or drops it', tbl)); END IF;
  IF NOT EXISTS (SELECT 1 FROM loom_guard_{S}.scoped_table WHERE rel = tbl) THEN
    PERFORM loom_guard_{S}.refuse(format('%s is not registered', tbl)); END IF;
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
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('registered tables without forced RLS: %s', bad)); END IF;
  SELECT string_agg(DISTINCT t.rel::text, ', ') INTO bad FROM loom_guard_{S}.scoped_table t
    WHERE (SELECT count(*) FROM pg_policy p WHERE p.polrelid = t.rel) <> (SELECT count(*) FROM loom_guard_{S}.scoped_policy sp WHERE sp.rel = t.rel)
       OR EXISTS (SELECT 1 FROM loom_guard_{S}.scoped_policy sp
                  LEFT JOIN pg_policies p ON p.policyname = sp.policyname AND p.tablename = (SELECT relname FROM pg_class WHERE oid = t.rel) AND p.schemaname = '{S}'
                  WHERE sp.rel = t.rel AND (p.qual IS DISTINCT FROM sp.qual OR p.with_check IS DISTINCT FROM sp.with_check
                        OR p.roles IS DISTINCT FROM sp.roles OR p.cmd IS DISTINCT FROM sp.cmd OR p.permissive IS DISTINCT FROM 'PERMISSIVE'));
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('policy set differs from the registered canonical set: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM loom_guard_{S}.scoped_table t JOIN pg_class c ON c.oid = t.rel, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT coalesce(
          (g.rolname = ANY (ARRAY[{BYPASS_LIST}]::name[]) AND a.privilege_type IN (SELECT privilege FROM loom_guard_{S}.commands()))
       OR EXISTS (SELECT 1 FROM loom_guard_{S}.commands() cm
                  WHERE cm.privilege = a.privilege_type AND cm.privilege = ANY (t.privileges) AND g.rolname = loom_guard_{S}.group_role(cm.is_write)), false);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('grant outside the whitelist: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || '.' || at.attname, ', ') INTO bad FROM loom_guard_{S}.scoped_table t JOIN pg_class c ON c.oid = t.rel JOIN pg_attribute at ON at.attrelid = c.oid WHERE at.attacl IS NOT NULL;
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('column grants are not allowed: %s', bad)); END IF;
  SELECT string_agg(c.relname, ', ') INTO bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner
    WHERE n.nspname = '{S}' AND c.relkind IN ('r','p','v','m','S') AND (r.rolbypassrls OR r.rolsuper OR r.rolname <> loom_guard_{S}.owner_role());
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('relations not owned by {OWNER}: %s', bad)); END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{S}' AND c.relkind = 'm') THEN
    PERFORM loom_guard_{S}.violation('materialized views are out of scope in a scoped schema'); END IF;
  SELECT string_agg(DISTINCT rw.ev_class::regclass::text, ', ') INTO bad FROM pg_rewrite rw JOIN loom_guard_{S}.scoped_table t ON t.rel = rw.ev_class WHERE rw.rulename <> '_RETURN';
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('rules on scoped tables are not allowed: %s', bad)); END IF;
  IF EXISTS (SELECT 1 FROM pg_roles m WHERE NOT m.rolbypassrls AND NOT m.rolsuper
               AND (loom_guard_{S}.is_member(m.rolname, loom_guard_{S}.group_role(false)) OR loom_guard_{S}.is_member(m.rolname, loom_guard_{S}.group_role(true)))
               AND EXISTS (SELECT 1 FROM pg_roles b WHERE b.rolbypassrls AND loom_guard_{S}.is_member(m.rolname, b.rolname)))
  THEN PERFORM loom_guard_{S}.violation('a non-bypass user is a member of a bypass role'); END IF;
  SELECT string_agg(m.rolname, ', ') INTO bad FROM pg_roles m
    WHERE NOT m.rolsuper AND m.rolname NOT IN (loom_guard_{S}.owner_role(), '{MIGRATOR}') AND loom_guard_{S}.is_member(m.rolname, loom_guard_{S}.owner_role());
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('only the migrator may be a member of {OWNER}: %s', bad)); END IF;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM loom_guard_{S}.scoped_table t WHERE NOT EXISTS (
    SELECT 1 FROM pg_trigger g WHERE g.tgrelid = t.rel AND g.tgname = 'loom_deny_owner_dml' AND g.tgenabled = 'A'
      AND g.tgfoid = 'loom_guard_{S}.deny_owner_dml'::regproc AND (g.tgtype & 2) <> 0 AND (g.tgtype & 28) = 28 AND (g.tgtype & 32) <> 0 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('owner trigger missing, disabled, wrong function or wrong events: %s', bad)); END IF;
  SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO bad FROM pg_index i JOIN loom_guard_{S}.scoped_table t ON t.rel = i.indrelid
    WHERE (i.indisunique OR i.indisexclusion)
      AND loom_guard_{S}.boundary_attnum(t.rel, t.scopes) <> ALL ((i.indkey::int2[])[0:i.indnkeyatts - 1]);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('unique index without the boundary column: %s', bad)); END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con
    JOIN loom_guard_{S}.scoped_table t ON t.rel = con.conrelid JOIN loom_guard_{S}.scoped_table t2 ON t2.rel = con.confrelid
    WHERE con.contype = 'f' AND (
      con.confdeltype IN ('n','d') OR con.confupdtype IN ('n','d')
      OR NOT EXISTS (SELECT 1 FROM generate_subscripts(con.conkey, 1) k
                     WHERE con.conkey[k] = loom_guard_{S}.boundary_attnum(t.rel, t.scopes)
                       AND con.confkey[k] = loom_guard_{S}.boundary_attnum(t2.rel, t2.scopes)));
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('FK between scoped tables must map boundary to boundary and never SET NULL/SET DEFAULT: %s', bad)); END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f' AND n.nspname = '{S}' AND con.confrelid IN (SELECT rel FROM loom_guard_{S}.scoped_table) AND con.conrelid NOT IN (SELECT rel FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('FK from an unscoped table into a scoped one: %s', bad)); END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i JOIN loom_guard_{S}.scoped_table t ON t.rel = i.inhparent WHERE i.inhrelid NOT IN (SELECT rel FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('unregistered partitions: %s', bad)); END IF;
END $$;
CREATE OR REPLACE FUNCTION loom_guard_{S}.on_ddl_end() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF loom_guard_{S}.hatch_open() THEN RETURN; END IF;
  PERFORM loom_guard_{S}.assert_scoped_schema();
END $$;
DROP EVENT TRIGGER IF EXISTS loom_guard_{S}_ddl;
CREATE EVENT TRIGGER loom_guard_{S}_ddl ON ddl_command_end EXECUTE FUNCTION loom_guard_{S}.on_ddl_end();
CREATE OR REPLACE FUNCTION loom_guard_{S}.on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE bad text;
BEGIN
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM loom_guard_{S}.scoped_table);
  IF bad IS NOT NULL THEN PERFORM loom_guard_{S}.violation(format('unprotect before dropping a registered table: %s', bad)); END IF;
END $$;
DROP EVENT TRIGGER IF EXISTS loom_guard_{S}_drop;
CREATE EVENT TRIGGER loom_guard_{S}_drop ON sql_drop EXECUTE FUNCTION loom_guard_{S}.on_sql_drop();
