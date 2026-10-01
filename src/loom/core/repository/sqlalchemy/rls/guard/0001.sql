CREATE TABLE config (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  app_schema name NOT NULL,
  owner_role name NOT NULL,
  migrator_role name NOT NULL,
  readers_role name NOT NULL,
  writers_role name NOT NULL,
  version_table name NOT NULL,
  data_version_table name NOT NULL,
  installer name NOT NULL,
  users jsonb NOT NULL
);
CREATE TABLE scoped_table (rel regclass PRIMARY KEY, scopes jsonb NOT NULL, privileges text[] NOT NULL);
CREATE TABLE scoped_policy (rel regclass, policyname name, cmd text, roles name[], qual text, with_check text, PRIMARY KEY (rel, policyname));
CREATE TABLE hatch (xid bigint PRIMARY KEY);
CREATE FUNCTION owner_functions() RETURNS name[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['protect_scoped_table', 'unprotect_scoped_table', 'open_hatch', 'assert_scoped_schema',
               'grant_table', 'grant_privileges', 'check_privileges', 'prepare_version_tables', 'limit_version_table',
               'registered_tables', 'guard_schema', 'app_schema', 'owner_role', 'group_role', 'users_of',
               'tagged', 'reject', 'refuse', 'is_member', 'commands', 'authorize']::name[] $$;
CREATE FUNCTION guard_schema() RETURNS name LANGUAGE sql STABLE AS $$
  SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = 'config'::regclass $$;
CREATE FUNCTION app_schema() RETURNS name LANGUAGE sql STABLE AS $$
  SELECT app_schema FROM config $$;
CREATE FUNCTION owner_role() RETURNS name LANGUAGE sql STABLE AS $$
  SELECT owner_role FROM config $$;
CREATE FUNCTION group_role(is_write boolean) RETURNS name LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN is_write THEN writers_role ELSE readers_role END FROM config $$;
CREATE FUNCTION users_of(users jsonb) RETURNS TABLE (name name, login boolean, access text, bypass boolean)
  LANGUAGE sql IMMUTABLE AS $$
  SELECT u.name, u.login, u.access, u.access = 'bypass' FROM jsonb_to_recordset(users) AS u(name name, login boolean, access text) $$;
CREATE FUNCTION login_roles() RETURNS name[] LANGUAGE sql STABLE AS $$
  SELECT ARRAY(SELECT migrator_role FROM config UNION ALL SELECT u.name FROM config, users_of(config.users) u WHERE u.login) $$;
CREATE FUNCTION bypass_roles() RETURNS name[] LANGUAGE sql STABLE AS $$
  SELECT ARRAY(SELECT u.name FROM config, users_of(config.users) u WHERE u.bypass) $$;
CREATE FUNCTION tagged(message text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT format('loom_guard[%s]: %s', coalesce(app_schema(), guard_schema()), message) $$;
CREATE FUNCTION reject(message text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE invalid_parameter_value USING MESSAGE = tagged(message);
END $$;
CREATE FUNCTION refuse(message text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE insufficient_privilege USING MESSAGE = tagged(message);
END $$;
CREATE FUNCTION violation(message text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'LG002', MESSAGE = tagged(message);
END $$;
CREATE FUNCTION is_member(member name, role_name name) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT pg_has_role(member, role_name, 'MEMBER') $$;
CREATE FUNCTION commands()
  RETURNS TABLE (privilege text, is_write boolean, uses_using boolean, uses_check boolean)
  LANGUAGE sql IMMUTABLE AS $$
  VALUES ('SELECT', false, true, false),
         ('INSERT', true, false, true),
         ('UPDATE', true, true, true),
         ('DELETE', true, true, false) $$;
CREATE FUNCTION reaches() RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['read', 'write', 'both'] $$;
CREATE FUNCTION reach(e jsonb) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(e->>'on', 'both') $$;
CREATE FUNCTION elevable(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce((e->>'elevable')::boolean, false) $$;
CREATE FUNCTION applies(e jsonb, for_reads boolean) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT reach(e) <> CASE WHEN for_reads THEN 'write' ELSE 'read' END $$;
CREATE FUNCTION is_boundary(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT applies(e, true) AND applies(e, false) AND NOT elevable(e) $$;
CREATE FUNCTION boundary_attnum(rel regclass, scopes jsonb) RETURNS int2 LANGUAGE sql STABLE AS $$
  SELECT a.attnum FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = rel AND a.attname = x->>'col'
  WHERE is_boundary(x) $$;
CREATE FUNCTION current_xid() RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT pg_current_xact_id()::text::bigint $$;
CREATE FUNCTION hatch_open() RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (SELECT 1 FROM hatch WHERE xid = current_xid()) $$;
CREATE FUNCTION enter_hatch() RETURNS void LANGUAGE sql AS $$
  INSERT INTO hatch VALUES (current_xid()) ON CONFLICT DO NOTHING $$;
CREATE FUNCTION leave_hatch() RETURNS void LANGUAGE sql AS $$
  DELETE FROM hatch WHERE xid = current_xid() $$;
CREATE FUNCTION open_hatch() RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF NOT is_member(session_user, owner_role()) THEN
    PERFORM refuse('only the owner may open the hatch'); END IF;
  PERFORM enter_hatch();
END $$;
CREATE FUNCTION term(col name, scope text, elevable boolean, coltype text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN elevable THEN format('(%s OR current_setting(%L, true) = %L)', e.equality, k.setting_name || '.any', 'on') ELSE e.equality END
  FROM (SELECT 'loom.scope.' || scope AS setting_name) k,
       LATERAL (SELECT format('%I = NULLIF(current_setting(%L, true), %L)::%s', col, k.setting_name, '', coltype) AS equality) e $$;
CREATE FUNCTION predicate(tbl regclass, scopes jsonb, for_reads boolean) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(term((x->>'col')::name, x->>'scope', elevable(x), format_type(a.atttypid, NULL)), ' AND ')
  FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
  WHERE applies(x, for_reads) $$;
CREATE FUNCTION deny_owner_dml() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
  IF pg_has_role(current_user, (SELECT relowner FROM pg_class WHERE oid = TG_RELID), 'USAGE')
     AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
  THEN RAISE EXCEPTION 'loom_guard[%]: owner DML on scoped table %.% is denied; run data migrations as the bypass user', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_NAME USING ERRCODE = 'LG001'; END IF;
  RETURN NULL;
END $$;
CREATE FUNCTION authorize(tbl regclass, action text) RETURNS void LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF (SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = tbl) IS DISTINCT FROM app_schema() THEN
    PERFORM refuse(format('%s is not in schema %s', tbl, app_schema())); END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT relowner FROM pg_class WHERE oid = tbl)) IS DISTINCT FROM owner_role()
     OR (SELECT rolname FROM pg_roles WHERE oid = (SELECT nspowner FROM pg_namespace WHERE nspname = app_schema())) IS DISTINCT FROM owner_role()
     OR NOT is_member(session_user, owner_role()) THEN
    PERFORM refuse(format('only the owner may %s %s', action, tbl)); END IF;
END $$;
CREATE FUNCTION check_privileges(privileges text[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT privileges <@ ARRAY(SELECT privilege FROM commands()) THEN
    PERFORM reject('privilege outside the closed set'); END IF;
END $$;
CREATE FUNCTION grant_privileges(tbl regclass, readers_privileges text[], writers_privileges text[]) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE target record; seq regclass;
BEGIN
  PERFORM check_privileges(readers_privileges || writers_privileges);
  FOR target IN SELECT group_role(g.is_write) AS role_name, g.privs
                FROM (VALUES (false, readers_privileges), (true, writers_privileges)) AS g(is_write, privs)
                WHERE cardinality(g.privs) > 0 LOOP
    EXECUTE format('GRANT %s ON %s TO %I', array_to_string(target.privs, ', '), tbl, target.role_name);
    FOR seq IN SELECT sq.oid::regclass FROM pg_depend d JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S'
               WHERE d.refobjid = tbl AND d.deptype = 'a' AND 'INSERT' = ANY (target.privs) LOOP
      EXECUTE format('GRANT USAGE ON SEQUENCE %s TO %I', seq, target.role_name);
    END LOOP;
  END LOOP;
END $$;
CREATE FUNCTION protect_scoped_table(tbl regclass, scopes jsonb, table_privileges text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE e jsonb; scope_name text; command record; boundary_count int;
BEGIN
  PERFORM authorize(tbl, 'protect');
  PERFORM check_privileges(table_privileges);
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl) THEN
    PERFORM refuse(format('%s already has policies; protect never adopts existing ones', tbl)); END IF;
  IF jsonb_typeof(scopes) <> 'array' OR jsonb_array_length(scopes) = 0 THEN
    PERFORM reject('scopes must be a non-empty array'); END IF;
  FOR e IN SELECT * FROM jsonb_array_elements(scopes) LOOP
    scope_name := e->>'scope';
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = e->>'col' AND attnum > 0 AND NOT attisdropped) THEN
      PERFORM reject(format('scope column %s does not exist on %s', e->>'col', tbl)); END IF;
    IF NOT reach(e) = ANY (reaches()) THEN
      PERFORM reject(format('scope reach %s is not read/write/both', e->>'on')); END IF;
    IF coalesce(scope_name, '') !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
      PERFORM reject(format('scope name %s is not an identifier', scope_name)); END IF;
  END LOOP;
  SELECT count(*) INTO boundary_count FROM jsonb_array_elements(scopes) x WHERE is_boundary(x);
  IF boundary_count <> 1 THEN
    PERFORM reject(format('a scoped table needs exactly one non-elevable boundary scope (got %s)', boundary_count)); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x WHERE elevable(x) AND applies(x, true)) THEN
    PERFORM reject('elevable scopes must be write-only'); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
             WHERE is_boundary(x) AND NOT a.attnotnull) THEN
    PERFORM reject('boundary column must be NOT NULL'); END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
             LEFT JOIN pg_collation co ON co.oid = a.attcollation
             WHERE a.atttypid = 'bpchar'::regtype OR NOT coalesce(co.collisdeterministic, true)) THEN
    PERFORM reject('scope columns may not be char(n) or use a nondeterministic collation: equal values must be identical'); END IF;
  IF EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = tbl AND (i.indisunique OR i.indisexclusion)
             AND boundary_attnum(tbl, scopes) <> ALL ((i.indkey::int2[])[0:i.indnkeyatts - 1])) THEN
    PERFORM violation(format('every unique index of %s must contain the boundary column', tbl)); END IF;
  PERFORM enter_hatch();
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
  EXECUTE format('CREATE POLICY loom_select ON %s FOR SELECT TO PUBLIC USING (%s)', tbl, predicate(tbl, scopes, true));
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC, %I, %I', tbl, group_role(false), group_role(true));
  FOR command IN SELECT * FROM commands() c WHERE c.is_write AND c.privilege = ANY (table_privileges) LOOP
    EXECUTE format('CREATE POLICY %I ON %s FOR %s TO PUBLIC%s%s', 'loom_' || lower(command.privilege), tbl, command.privilege,
      CASE WHEN command.uses_using THEN format(' USING (%s)', predicate(tbl, scopes, false)) ELSE '' END,
      CASE WHEN command.uses_check THEN format(' WITH CHECK (%s)', predicate(tbl, scopes, false)) ELSE '' END);
  END LOOP;
  PERFORM grant_privileges(tbl,
    ARRAY(SELECT c.privilege FROM commands() c WHERE NOT c.is_write AND c.privilege = ANY (table_privileges)),
    ARRAY(SELECT c.privilege FROM commands() c WHERE c.is_write AND c.privilege = ANY (table_privileges)));
  EXECUTE format('CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %I.deny_owner_dml()', tbl, guard_schema());
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_deny_owner_dml', tbl);
  INSERT INTO scoped_table VALUES (tbl, scopes, table_privileges);
  INSERT INTO scoped_policy SELECT tbl, policyname, cmd, roles, qual, with_check FROM pg_policies p JOIN pg_class c ON c.relname = p.tablename JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname WHERE c.oid = tbl;
  PERFORM leave_hatch();
END $$;
CREATE FUNCTION unprotect_scoped_table(tbl regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE pol record;
BEGIN
  PERFORM authorize(tbl, 'unprotect');
  IF NOT hatch_open() THEN
    PERFORM violation(format('unprotect %s only under the hatch, in the transaction that changes or drops it', tbl)); END IF;
  IF NOT EXISTS (SELECT 1 FROM scoped_table WHERE rel = tbl) THEN
    PERFORM refuse(format('%s is not registered', tbl)); END IF;
  DELETE FROM scoped_policy WHERE rel = tbl;
  DELETE FROM scoped_table WHERE rel = tbl;
  FOR pol IN SELECT * FROM (SELECT polname FROM pg_policy WHERE polrelid = tbl ORDER BY polname) q LOOP
    EXECUTE format('DROP POLICY %I ON %s', pol.polname, tbl);
  END LOOP;
  EXECUTE format('DROP TRIGGER IF EXISTS loom_deny_owner_dml ON %s', tbl);
  EXECUTE format('ALTER TABLE %s NO FORCE ROW LEVEL SECURITY, DISABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('REVOKE ALL ON %s FROM %I, %I', tbl, group_role(false), group_role(true));
END $$;
CREATE FUNCTION grant_table(tbl regclass, readers_privileges text[], writers_privileges text[]) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM authorize(tbl, 'grant on');
  PERFORM grant_privileges(tbl, readers_privileges, writers_privileges);
END $$;
CREATE FUNCTION limit_version_table(c config) RETURNS void LANGUAGE plpgsql AS $$
DECLARE bypass name;
BEGIN
  IF to_regclass(format('%I.%I', c.app_schema, c.version_table)) IS NULL THEN RETURN; END IF;
  FOR bypass IN SELECT u.name FROM users_of(c.users) u WHERE u.bypass LOOP
    EXECUTE format('REVOKE ALL ON %I.%I FROM %I', c.app_schema, c.version_table, bypass);
    EXECUTE format('GRANT SELECT ON %I.%I TO %I', c.app_schema, c.version_table, bypass);
  END LOOP;
END $$;
CREATE FUNCTION prepare_version_tables() RETURNS void LANGUAGE plpgsql AS $$
DECLARE c config; tbl name;
BEGIN
  SELECT * INTO c FROM config;
  IF NOT is_member(session_user, c.owner_role) THEN PERFORM refuse('only the owner may prepare the version tables'); END IF;
  FOREACH tbl IN ARRAY ARRAY[c.version_table, c.data_version_table] LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (version_num VARCHAR(32) NOT NULL, CONSTRAINT %I PRIMARY KEY (version_num))',
                   c.app_schema, tbl, tbl || '_pkc');
  END LOOP;
  PERFORM limit_version_table(c);
END $$;
CREATE FUNCTION registered_tables() RETURNS SETOF name LANGUAGE sql STABLE AS $$
  SELECT c.relname FROM scoped_table t JOIN pg_class c ON c.oid = t.rel $$;
CREATE FUNCTION assert_scoped_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE bad text; cfg config; app oid; guard oid;
BEGIN
  SELECT * INTO cfg FROM config;
  SELECT oid INTO app FROM pg_namespace WHERE nspname = cfg.app_schema;
  SELECT oid INTO guard FROM pg_namespace WHERE nspname = guard_schema();
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t JOIN pg_class c ON c.oid = t.rel WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity);
  IF bad IS NOT NULL THEN PERFORM violation(format('registered tables without forced RLS: %s', bad)); END IF;
  SELECT string_agg(DISTINCT t.rel::text, ', ') INTO bad FROM scoped_table t
    WHERE (SELECT count(*) FROM pg_policy p WHERE p.polrelid = t.rel) <> (SELECT count(*) FROM scoped_policy sp WHERE sp.rel = t.rel)
       OR EXISTS (SELECT 1 FROM scoped_policy sp
                  LEFT JOIN pg_policies p ON p.policyname = sp.policyname AND p.tablename = (SELECT relname FROM pg_class WHERE oid = t.rel) AND p.schemaname = cfg.app_schema
                  WHERE sp.rel = t.rel AND (p.qual IS DISTINCT FROM sp.qual OR p.with_check IS DISTINCT FROM sp.with_check
                        OR p.roles IS DISTINCT FROM sp.roles OR p.cmd IS DISTINCT FROM sp.cmd OR p.permissive IS DISTINCT FROM 'PERMISSIVE'));
  IF bad IS NOT NULL THEN PERFORM violation(format('policy set differs from the registered canonical set: %s', bad)); END IF;
  SELECT string_agg(c.relname, ', ') INTO bad FROM pg_class c
    WHERE c.relnamespace = app AND c.relkind IN ('r','p') AND c.oid NOT IN (SELECT rel FROM scoped_table)
      AND (c.relrowsecurity OR c.relforcerowsecurity
           OR EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid)
           OR EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = c.oid AND g.tgname LIKE 'loom\_%'));
  IF bad IS NOT NULL THEN PERFORM violation(format('unregistered tables carry row-level security, policies or loom triggers: %s', bad)); END IF;
  SELECT string_agg(DISTINCT p.proname, ', ') INTO bad FROM pg_proc p
    WHERE p.pronamespace = app AND p.proname IN (SELECT g.proname FROM pg_proc g WHERE g.pronamespace = guard);
  IF bad IS NOT NULL THEN PERFORM violation(format('routines in %s collide with guard functions: %s', cfg.app_schema, bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM scoped_table t JOIN pg_class c ON c.oid = t.rel, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT coalesce(
          (g.rolname = ANY (bypass_roles()) AND a.privilege_type IN (SELECT privilege FROM commands()))
       OR EXISTS (SELECT 1 FROM commands() cm
                  WHERE cm.privilege = a.privilege_type AND cm.privilege = ANY (t.privileges) AND g.rolname = group_role(cm.is_write)), false);
  IF bad IS NOT NULL THEN PERFORM violation(format('grant outside the whitelist: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || '.' || at.attname, ', ') INTO bad FROM scoped_table t JOIN pg_class c ON c.oid = t.rel JOIN pg_attribute at ON at.attrelid = c.oid WHERE at.attacl IS NOT NULL;
  IF bad IS NOT NULL THEN PERFORM violation(format('column grants are not allowed: %s', bad)); END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT nspowner FROM pg_namespace WHERE oid = app)) IS DISTINCT FROM cfg.owner_role THEN
    PERFORM violation(format('schema %s is not owned by %s', cfg.app_schema, cfg.owner_role)); END IF;
  SELECT string_agg(c.relname, ', ') INTO bad FROM pg_class c JOIN pg_roles r ON r.oid = c.relowner
    WHERE c.relnamespace = app AND c.relkind IN ('r','p','v','m','S') AND (r.rolbypassrls OR r.rolsuper OR r.rolname <> cfg.owner_role);
  IF bad IS NOT NULL THEN PERFORM violation(format('relations not owned by %s: %s', cfg.owner_role, bad)); END IF;
  IF EXISTS (SELECT 1 FROM pg_class c WHERE c.relnamespace = app AND c.relkind = 'm') THEN
    PERFORM violation('materialized views are out of scope in a scoped schema'); END IF;
  SELECT string_agg(DISTINCT rw.ev_class::regclass::text, ', ') INTO bad FROM pg_rewrite rw JOIN scoped_table t ON t.rel = rw.ev_class WHERE rw.rulename <> '_RETURN';
  IF bad IS NOT NULL THEN PERFORM violation(format('rules on scoped tables are not allowed: %s', bad)); END IF;
  IF EXISTS (SELECT 1 FROM pg_roles m WHERE NOT m.rolbypassrls AND NOT m.rolsuper
               AND (is_member(m.rolname, cfg.readers_role) OR is_member(m.rolname, cfg.writers_role))
               AND EXISTS (SELECT 1 FROM pg_roles b WHERE b.rolbypassrls AND is_member(m.rolname, b.rolname)))
  THEN PERFORM violation('a non-bypass user is a member of a bypass role'); END IF;
  SELECT string_agg(m.rolname, ', ') INTO bad FROM pg_roles m
    WHERE NOT m.rolsuper AND m.rolname NOT IN (cfg.owner_role, cfg.migrator_role) AND is_member(m.rolname, cfg.owner_role);
  IF bad IS NOT NULL THEN PERFORM violation(format('only the migrator may be a member of %s: %s', cfg.owner_role, bad)); END IF;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t WHERE NOT EXISTS (
    SELECT 1 FROM pg_trigger g WHERE g.tgrelid = t.rel AND g.tgname = 'loom_deny_owner_dml' AND g.tgenabled = 'A'
      AND g.tgfoid = to_regprocedure(quote_ident(guard_schema()) || '.deny_owner_dml()') AND (g.tgtype & 2) <> 0 AND (g.tgtype & 28) = 28 AND (g.tgtype & 32) <> 0 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
  IF bad IS NOT NULL THEN PERFORM violation(format('owner trigger missing, disabled, wrong function or wrong events: %s', bad)); END IF;
  SELECT string_agg(i.indexrelid::regclass::text, ', ') INTO bad FROM pg_index i JOIN scoped_table t ON t.rel = i.indrelid
    WHERE (i.indisunique OR i.indisexclusion)
      AND boundary_attnum(t.rel, t.scopes) <> ALL ((i.indkey::int2[])[0:i.indnkeyatts - 1]);
  IF bad IS NOT NULL THEN PERFORM violation(format('unique index without the boundary column: %s', bad)); END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con
    JOIN scoped_table t ON t.rel = con.conrelid JOIN scoped_table t2 ON t2.rel = con.confrelid
    WHERE con.contype = 'f' AND (
      con.confdeltype IN ('n','d') OR con.confupdtype IN ('n','d')
      OR NOT EXISTS (SELECT 1 FROM generate_subscripts(con.conkey, 1) k
                     WHERE con.conkey[k] = boundary_attnum(t.rel, t.scopes)
                       AND con.confkey[k] = boundary_attnum(t2.rel, t2.scopes)));
  IF bad IS NOT NULL THEN PERFORM violation(format('FK between scoped tables must map boundary to boundary and never SET NULL/SET DEFAULT: %s', bad)); END IF;
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
    WHERE con.contype = 'f' AND c.relnamespace = app AND con.confrelid IN (SELECT rel FROM scoped_table) AND con.conrelid NOT IN (SELECT rel FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('FK from an unscoped table into a scoped one: %s', bad)); END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i JOIN scoped_table t ON t.rel = i.inhparent WHERE i.inhrelid NOT IN (SELECT rel FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unregistered partitions: %s', bad)); END IF;
END $$;
CREATE FUNCTION close_hatch() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  DELETE FROM hatch WHERE xid = NEW.xid;
  PERFORM assert_scoped_schema();
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER close_hatch AFTER INSERT ON hatch DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION close_hatch();
ALTER TABLE hatch ENABLE ALWAYS TRIGGER close_hatch;
CREATE FUNCTION forbid_guard_ddl(touched boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF touched AND current_setting(guard_schema() || '.installing', true) IS DISTINCT FROM 'on' THEN
    PERFORM violation(format('DDL on guard schema %s outside the loom installer', guard_schema())); END IF;
END $$;
CREATE FUNCTION on_ddl_end() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  PERFORM forbid_guard_ddl(EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() c WHERE c.schema_name = guard_schema()));
  IF hatch_open() OR NOT EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() c
                                 WHERE c.schema_name IS NULL OR c.schema_name IN (guard_schema(), app_schema())) THEN
    RETURN; END IF;
  PERFORM assert_scoped_schema();
END $$;
CREATE FUNCTION on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE bad text;
BEGIN
  PERFORM forbid_guard_ddl(EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() d
                                   WHERE d.schema_name = guard_schema() OR d.object_identity = quote_ident(guard_schema())));
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unprotect before dropping a registered table: %s', bad)); END IF;
END $$;
CREATE FUNCTION set_password_verifier(role_name name, verifier text) RETURNS void LANGUAGE plpgsql SET log_statement = 'none' AS $$
BEGIN
  IF verifier !~ '^SCRAM-SHA-256\$[0-9]+:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$' THEN
    PERFORM reject(format('password for %s must be a SCRAM-SHA-256 verifier', role_name)); END IF;
  IF NOT role_name = ANY (login_roles()) THEN
    PERFORM refuse(format('%s is not a login user of schema %s', role_name, app_schema())); END IF;
  EXECUTE format('ALTER ROLE %I PASSWORD %L', role_name, verifier);
END $$;
CREATE FUNCTION role_specs(c config) RETURNS TABLE (name name, login boolean, bypass boolean, inherit boolean)
  LANGUAGE sql IMMUTABLE AS $$
  VALUES (c.owner_role, false, false, true), (c.migrator_role, true, false, false),
         (c.readers_role, false, false, true), (c.writers_role, false, false, true)
  UNION ALL SELECT u.name, u.login, u.bypass, NOT u.bypass FROM users_of(c.users) u $$;
CREATE FUNCTION expected_members(c config) RETURNS TABLE (role_name name, member name) LANGUAGE sql IMMUTABLE AS $$
  SELECT c.owner_role, c.migrator_role
  UNION ALL SELECT g.role_name, u.name FROM users_of(c.users) u
    CROSS JOIN LATERAL (VALUES (c.readers_role, true), (c.writers_role, u.access <> 'read')) AS g(role_name, wanted)
    WHERE NOT u.bypass AND g.wanted $$;
CREATE FUNCTION unexpected_memberships(c config) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(format('%s in %s%s', m.rolname, r.rolname, CASE WHEN am.admin_option THEN ' with admin option' ELSE '' END), ', ')
  FROM pg_auth_members am JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles m ON m.oid = am.member
  WHERE (r.rolname IN (SELECT s.name FROM role_specs(c) s) OR m.rolname IN (SELECT s.name FROM role_specs(c) s))
    AND NOT m.rolsuper AND m.rolname <> c.installer
    AND (am.admin_option OR (r.rolname, m.rolname) NOT IN (SELECT e.role_name, e.member FROM expected_members(c) e)) $$;
CREATE FUNCTION claim_role(role_name name, login boolean, bypass boolean, inherit boolean, recorded boolean) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE r pg_roles%ROWTYPE;
BEGIN
  IF role_name LIKE 'pg\_%' OR role_name LIKE 'rds\_%' THEN
    PERFORM refuse(format('role %s uses a reserved prefix', role_name)); END IF;
  SELECT * INTO r FROM pg_roles WHERE rolname = role_name;
  IF NOT FOUND THEN
    EXECUTE format('CREATE ROLE %I %s %s %s', role_name,
      CASE WHEN login THEN 'LOGIN' ELSE 'NOLOGIN' END,
      CASE WHEN bypass THEN 'BYPASSRLS' ELSE 'NOBYPASSRLS' END,
      CASE WHEN inherit THEN 'INHERIT' ELSE 'NOINHERIT' END);
  ELSIF NOT recorded THEN
    PERFORM refuse(format('role %s exists and this guard did not create it; the bootstrap never adopts a role', role_name));
  ELSIF (r.rolcanlogin, r.rolbypassrls, r.rolinherit) IS DISTINCT FROM (login, bypass, inherit)
        OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication THEN
    PERFORM refuse(format('role %s already exists with different attributes', role_name));
  END IF;
END $$;
CREATE FUNCTION grant_bypass(c config, role_name name) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', c.app_schema, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', c.owner_role, c.app_schema, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE ON SEQUENCES TO %I', c.owner_role, c.app_schema, role_name);
  EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I', c.app_schema, role_name);
  EXECUTE format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA %I TO %I', c.app_schema, role_name);
END $$;
CREATE FUNCTION retire_user(c config, role_name name) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('REVOKE %I, %I FROM %I', c.readers_role, c.writers_role, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I REVOKE ALL ON TABLES FROM %I', c.owner_role, c.app_schema, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I REVOKE ALL ON SEQUENCES FROM %I', c.owner_role, c.app_schema, role_name);
  EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA %I FROM %I', c.app_schema, role_name);
  EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %I FROM %I', c.app_schema, role_name);
  EXECUTE format('REVOKE ALL ON SCHEMA %I FROM %I', c.app_schema, role_name);
  EXECUTE format('ALTER ROLE %I RESET ALL', role_name);
END $$;
CREATE FUNCTION secure_guard(c config) RETURNS void LANGUAGE plpgsql AS $$
DECLARE guard name := guard_schema(); fn regprocedure; t record;
BEGIN
  EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', guard, c.owner_role);
  EXECUTE format('GRANT SELECT ON config, scoped_table, scoped_policy TO %I', c.owner_role);
  FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = guard AND p.proname = ANY (owner_functions()) LOOP
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', fn, c.owner_role);
  END LOOP;
  FOR t IN SELECT guard || v.suffix AS trigger_name, v.event, v.handler
           FROM (VALUES ('_ddl', 'ddl_command_end', 'on_ddl_end'), ('_drop', 'sql_drop', 'on_sql_drop')) AS v(suffix, event, handler) LOOP
    EXECUTE format('DROP EVENT TRIGGER IF EXISTS %I', t.trigger_name);
    EXECUTE format('CREATE EVENT TRIGGER %I ON %s EXECUTE FUNCTION %I.%I()', t.trigger_name, t.event, guard, t.handler);
    EXECUTE format('ALTER EVENT TRIGGER %I ENABLE ALWAYS', t.trigger_name);
  END LOOP;
END $$;
CREATE FUNCTION configure(doc jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  wanted config := jsonb_populate_record(NULL::config, doc);
  cfg config;
  configured boolean;
  spec record;
  holder name;
  bad text;
BEGIN
  wanted.singleton := true;
  wanted.installer := current_user;
  SELECT * INTO cfg FROM config;
  configured := FOUND;
  IF configured AND to_jsonb(cfg) - 'users' IS DISTINCT FROM to_jsonb(wanted) - 'users' THEN
    PERFORM refuse(format('guard %s is configured for another schema, other roles, other version tables or another installer', guard_schema())); END IF;
  FOR spec IN SELECT s.*, configured AND s.name IN (SELECT o.name FROM role_specs(cfg) o) AS recorded FROM role_specs(wanted) s LOOP
    PERFORM claim_role(spec.name, spec.login, spec.bypass, spec.inherit, spec.recorded);
  END LOOP;
  FOR spec IN SELECT o.name FROM users_of(cfg.users) o WHERE o.name NOT IN (SELECT u.name FROM users_of(wanted.users) u) LOOP
    PERFORM retire_user(wanted, spec.name);
  END LOOP;
  FOR spec IN SELECT * FROM expected_members(wanted) LOOP
    EXECUTE format('GRANT %I TO %I', spec.role_name, spec.member);
  END LOOP;
  EXECUTE format('ALTER ROLE %I SET role = %I', wanted.migrator_role, wanted.owner_role);
  FOR spec IN SELECT s.name, CASE WHEN s.name IN (wanted.owner_role, wanted.migrator_role)
                                  THEN format('%I, %I', wanted.app_schema, guard_schema()) ELSE quote_ident(wanted.app_schema) END AS path
              FROM role_specs(wanted) s WHERE s.login OR s.name = wanted.owner_role LOOP
    EXECUTE format('ALTER ROLE %I SET search_path = %s', spec.name, spec.path);
  END LOOP;
  SELECT r.rolname INTO holder FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = wanted.app_schema;
  IF NOT FOUND THEN
    EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', wanted.app_schema, wanted.owner_role);
  ELSIF holder IS DISTINCT FROM wanted.owner_role THEN
    PERFORM refuse(format('schema %s exists and is not owned by %s; the bootstrap never adopts it', wanted.app_schema, wanted.owner_role));
  END IF;
  IF coalesce((doc->>'revoke_public')::boolean, false) THEN EXECUTE 'REVOKE ALL ON SCHEMA public FROM PUBLIC'; END IF;
  EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC', wanted.app_schema);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I, %I', wanted.app_schema, wanted.readers_role, wanted.writers_role);
  FOR spec IN SELECT u.name FROM users_of(wanted.users) u WHERE u.bypass LOOP
    PERFORM grant_bypass(wanted, spec.name);
  END LOOP;
  PERFORM limit_version_table(wanted);
  INSERT INTO config SELECT (wanted).* ON CONFLICT (singleton) DO UPDATE SET users = EXCLUDED.users;
  bad := unexpected_memberships(wanted);
  IF bad IS NOT NULL THEN PERFORM refuse(format('unexpected role memberships: %s', bad)); END IF;
  PERFORM secure_guard(wanted);
END $$;
DO $$
DECLARE fn regprocedure;
BEGIN
  FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p WHERE p.pronamespace = current_schema()::regnamespace LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = pg_catalog, %I, pg_temp', fn, current_schema());
  END LOOP;
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', current_schema());
END $$;
