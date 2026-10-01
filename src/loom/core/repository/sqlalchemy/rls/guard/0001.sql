CREATE TABLE config (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  app_schema name NOT NULL,
  owner_role name NOT NULL,
  migrator_role name NOT NULL,
  readers_role name NOT NULL,
  writers_role name NOT NULL,
  bypass_roles name[] NOT NULL,
  login_roles name[] NOT NULL,
  version_table name NOT NULL,
  data_version_table name NOT NULL
);
CREATE TABLE scoped_table (rel regclass PRIMARY KEY, scopes jsonb NOT NULL, privileges text[] NOT NULL);
CREATE TABLE scoped_policy (rel regclass, policyname name, cmd text, roles name[], qual text, with_check text, PRIMARY KEY (rel, policyname));
CREATE FUNCTION owner_functions() RETURNS name[] LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT ARRAY['protect_scoped_table', 'unprotect_scoped_table', 'open_hatch', 'assert_scoped_schema',
               'grant_table', 'prepare_version_tables', 'registered_tables', 'owner_functions',
               'app_schema', 'owner_role', 'group_role', 'fail', 'reject', 'refuse', 'violation',
               'is_member', 'commands', 'authorize', 'set_hatch']::name[] $$;
CREATE FUNCTION app_schema() RETURNS name LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT app_schema FROM config $$;
CREATE FUNCTION owner_role() RETURNS name LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT owner_role FROM config $$;
CREATE FUNCTION group_role(is_write boolean) RETURNS name LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT CASE WHEN is_write THEN writers_role ELSE readers_role END FROM config $$;
CREATE FUNCTION fail(code text, message text) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  RAISE EXCEPTION 'loom_guard[%]: %', coalesce(app_schema(), current_schema()), message USING ERRCODE = code;
END $$;
CREATE FUNCTION reject(message text) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT fail('22023', message) $$;
CREATE FUNCTION refuse(message text) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT fail('42501', message) $$;
CREATE FUNCTION violation(message text) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT fail('LG002', message) $$;
CREATE FUNCTION is_member(member name, role name) RETURNS boolean LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT pg_has_role(member, role, 'MEMBER') $$;
CREATE FUNCTION commands()
  RETURNS TABLE (privilege text, is_write boolean, uses_using boolean, uses_check boolean, needs_sequences boolean)
  LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  VALUES ('SELECT', false, true, false, false),
         ('INSERT', true, false, true, true),
         ('UPDATE', true, true, true, false),
         ('DELETE', true, true, false, false) $$;
CREATE FUNCTION reaches() RETURNS text[] LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT ARRAY['read', 'write', 'both'] $$;
CREATE FUNCTION reach(e jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT coalesce(e->>'on', 'both') $$;
CREATE FUNCTION elevable(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT coalesce((e->>'elevable')::boolean, false) $$;
CREATE FUNCTION applies(e jsonb, for_reads boolean) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT reach(e) <> CASE WHEN for_reads THEN 'write' ELSE 'read' END $$;
CREATE FUNCTION is_boundary(e jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT applies(e, true) AND applies(e, false) AND NOT elevable(e) $$;
CREATE FUNCTION boundary_attnum(rel regclass, scopes jsonb) RETURNS int2 LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT a.attnum FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = rel AND a.attname = x->>'col'
  WHERE is_boundary(x) $$;
CREATE FUNCTION set_hatch(state text) RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT set_config(current_schema() || '.protecting', state, true) $$;
CREATE FUNCTION hatch_open() RETURNS boolean LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT coalesce(current_setting(current_schema() || '.protecting', true) = 'on', false) $$;
CREATE FUNCTION open_hatch() RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS $$
  SELECT set_hatch('on') $$;
CREATE FUNCTION term(col name, scope text, elevable boolean, coltype text) RETURNS text LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
  SELECT CASE WHEN elevable THEN format('(%s OR current_setting(%L, true) = %L)', e.equality, k.setting_name || '.any', 'on') ELSE e.equality END
  FROM (SELECT 'loom.scope.' || scope AS setting_name) k,
       LATERAL (SELECT format('%I = NULLIF(current_setting(%L, true), %L)::%s', col, k.setting_name, '', coltype) AS equality) e $$;
CREATE FUNCTION predicate(tbl regclass, scopes jsonb, for_reads boolean) RETURNS text LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT string_agg(term((x->>'col')::name, x->>'scope', elevable(x), format_type(a.atttypid, NULL)), ' AND ')
  FROM jsonb_array_elements(scopes) x JOIN pg_attribute a ON a.attrelid = tbl AND a.attname = x->>'col'
  WHERE applies(x, for_reads) $$;
CREATE FUNCTION deny_owner_dml() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN RETURN NULL; END IF;
  IF pg_has_role(current_user, (SELECT relowner FROM pg_class WHERE oid = TG_RELID), 'USAGE')
     AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
  THEN RAISE EXCEPTION 'loom_guard[%]: owner DML on scoped table %.% is denied; run data migrations as the bypass user', TG_TABLE_SCHEMA, TG_TABLE_SCHEMA, TG_TABLE_NAME USING ERRCODE = 'LG001'; END IF;
  RETURN NULL;
END $$;
CREATE FUNCTION authorize(tbl regclass, action text) RETURNS void LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
BEGIN
  IF (SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = tbl) IS DISTINCT FROM app_schema() THEN
    PERFORM refuse(format('%s is not in schema %s', tbl, app_schema())); END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT relowner FROM pg_class WHERE oid = tbl)) IS DISTINCT FROM owner_role()
     OR (SELECT rolname FROM pg_roles WHERE oid = (SELECT nspowner FROM pg_namespace WHERE nspname = app_schema())) IS DISTINCT FROM owner_role()
     OR NOT is_member(session_user, owner_role()) THEN
    PERFORM refuse(format('only the owner may %s %s', action, tbl)); END IF;
END $$;
CREATE FUNCTION protect_scoped_table(tbl regclass, scopes jsonb, table_privileges text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE e jsonb; scope_name text; command record; seq record; boundary_count int;
BEGIN
  PERFORM authorize(tbl, 'protect');
  IF EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl) THEN
    PERFORM refuse(format('%s already has policies; protect never adopts existing ones', tbl)); END IF;
  IF NOT (table_privileges <@ ARRAY(SELECT privilege FROM commands())) THEN
    PERFORM reject('privilege outside the closed set'); END IF;
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
  PERFORM set_hatch('on');
  EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
  EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
  EXECUTE format('CREATE POLICY loom_select ON %s FOR SELECT TO PUBLIC USING (%s)', tbl, predicate(tbl, scopes, true));
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC, %I, %I', tbl, group_role(false), group_role(true));
  FOR command IN SELECT * FROM commands() c WHERE c.privilege = ANY (table_privileges) LOOP
    IF command.is_write THEN
      EXECUTE format('CREATE POLICY %I ON %s FOR %s TO PUBLIC%s%s', 'loom_' || lower(command.privilege), tbl, command.privilege,
        CASE WHEN command.uses_using THEN format(' USING (%s)', predicate(tbl, scopes, false)) ELSE '' END,
        CASE WHEN command.uses_check THEN format(' WITH CHECK (%s)', predicate(tbl, scopes, false)) ELSE '' END);
    END IF;
    EXECUTE format('GRANT %s ON %s TO %I', command.privilege, tbl, group_role(command.is_write));
    IF command.needs_sequences THEN
      FOR seq IN SELECT sq.oid::regclass AS rel FROM pg_depend d JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S' WHERE d.refobjid = tbl AND d.deptype = 'a' LOOP
        EXECUTE format('GRANT USAGE ON SEQUENCE %s TO %I', seq.rel, group_role(true));
      END LOOP;
    END IF;
  END LOOP;
  EXECUTE format('CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %I.deny_owner_dml()', tbl, current_schema());
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_deny_owner_dml', tbl);
  INSERT INTO scoped_table VALUES (tbl, scopes, table_privileges);
  INSERT INTO scoped_policy SELECT tbl, policyname, cmd, roles, qual, with_check FROM pg_policies p JOIN pg_class c ON c.relname = p.tablename JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname WHERE c.oid = tbl;
  PERFORM set_hatch('');
END $$;
CREATE FUNCTION unprotect_scoped_table(tbl regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
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
END $$;
CREATE FUNCTION grant_table(tbl regclass, readers_privileges text[], writers_privileges text[]) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE seq record;
BEGIN
  PERFORM authorize(tbl, 'grant on');
  IF NOT (readers_privileges <@ ARRAY(SELECT privilege FROM commands()) AND writers_privileges <@ ARRAY(SELECT privilege FROM commands())) THEN
    PERFORM reject('privilege outside the closed set'); END IF;
  IF cardinality(readers_privileges) > 0 THEN
    EXECUTE format('GRANT %s ON %s TO %I', array_to_string(readers_privileges, ', '), tbl, group_role(false)); END IF;
  IF cardinality(writers_privileges) > 0 THEN
    EXECUTE format('GRANT %s ON %s TO %I', array_to_string(writers_privileges, ', '), tbl, group_role(true)); END IF;
  FOR seq IN SELECT r.name, sq.oid::regclass AS rel FROM (VALUES (false, readers_privileges), (true, writers_privileges)) g(is_write, privs)
             CROSS JOIN LATERAL (SELECT group_role(g.is_write) AS name) r(name)
             JOIN pg_depend d ON d.refobjid = tbl AND d.deptype = 'a'
             JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S'
             WHERE 'INSERT' = ANY (g.privs) LOOP
    EXECUTE format('GRANT USAGE ON SEQUENCE %s TO %I', seq.rel, seq.name);
  END LOOP;
END $$;
CREATE FUNCTION prepare_version_tables() RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE cfg config%ROWTYPE; tbl name; bypass name;
BEGIN
  SELECT * INTO cfg FROM config;
  IF NOT is_member(session_user, cfg.owner_role) THEN PERFORM refuse('only the owner may prepare the version tables'); END IF;
  FOREACH tbl IN ARRAY ARRAY[cfg.version_table, cfg.data_version_table] LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (version_num VARCHAR(32) NOT NULL, CONSTRAINT %I PRIMARY KEY (version_num))',
                   cfg.app_schema, tbl, tbl || '_pkc');
  END LOOP;
  FOREACH bypass IN ARRAY cfg.bypass_roles LOOP
    EXECUTE format('REVOKE ALL ON %I.%I FROM %I', cfg.app_schema, cfg.version_table, bypass);
    EXECUTE format('GRANT SELECT ON %I.%I TO %I', cfg.app_schema, cfg.version_table, bypass);
  END LOOP;
END $$;
CREATE FUNCTION registered_tables() RETURNS SETOF name LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
  SELECT c.relname FROM scoped_table t JOIN pg_class c ON c.oid = t.rel $$;
CREATE FUNCTION assert_scoped_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE bad text; cfg config%ROWTYPE;
BEGIN
  SELECT * INTO cfg FROM config;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t JOIN pg_class c ON c.oid = t.rel WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity);
  IF bad IS NOT NULL THEN PERFORM violation(format('registered tables without forced RLS: %s', bad)); END IF;
  SELECT string_agg(DISTINCT t.rel::text, ', ') INTO bad FROM scoped_table t
    WHERE (SELECT count(*) FROM pg_policy p WHERE p.polrelid = t.rel) <> (SELECT count(*) FROM scoped_policy sp WHERE sp.rel = t.rel)
       OR EXISTS (SELECT 1 FROM scoped_policy sp
                  LEFT JOIN pg_policies p ON p.policyname = sp.policyname AND p.tablename = (SELECT relname FROM pg_class WHERE oid = t.rel) AND p.schemaname = cfg.app_schema
                  WHERE sp.rel = t.rel AND (p.qual IS DISTINCT FROM sp.qual OR p.with_check IS DISTINCT FROM sp.with_check
                        OR p.roles IS DISTINCT FROM sp.roles OR p.cmd IS DISTINCT FROM sp.cmd OR p.permissive IS DISTINCT FROM 'PERMISSIVE'));
  IF bad IS NOT NULL THEN PERFORM violation(format('policy set differs from the registered canonical set: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM scoped_table t JOIN pg_class c ON c.oid = t.rel, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT coalesce(
          (g.rolname = ANY (cfg.bypass_roles) AND a.privilege_type IN (SELECT privilege FROM commands()))
       OR EXISTS (SELECT 1 FROM commands() cm
                  WHERE cm.privilege = a.privilege_type AND cm.privilege = ANY (t.privileges) AND g.rolname = group_role(cm.is_write)), false);
  IF bad IS NOT NULL THEN PERFORM violation(format('grant outside the whitelist: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || '.' || at.attname, ', ') INTO bad FROM scoped_table t JOIN pg_class c ON c.oid = t.rel JOIN pg_attribute at ON at.attrelid = c.oid WHERE at.attacl IS NOT NULL;
  IF bad IS NOT NULL THEN PERFORM violation(format('column grants are not allowed: %s', bad)); END IF;
  IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT nspowner FROM pg_namespace WHERE nspname = cfg.app_schema)) IS DISTINCT FROM cfg.owner_role THEN
    PERFORM violation(format('schema %s is not owned by %s', cfg.app_schema, cfg.owner_role)); END IF;
  SELECT string_agg(c.relname, ', ') INTO bad FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner
    WHERE n.nspname = cfg.app_schema AND c.relkind IN ('r','p','v','m','S') AND (r.rolbypassrls OR r.rolsuper OR r.rolname <> cfg.owner_role);
  IF bad IS NOT NULL THEN PERFORM violation(format('relations not owned by %s: %s', cfg.owner_role, bad)); END IF;
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = cfg.app_schema AND c.relkind = 'm') THEN
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
      AND g.tgfoid = to_regprocedure(quote_ident(current_schema()) || '.deny_owner_dml()') AND (g.tgtype & 2) <> 0 AND (g.tgtype & 28) = 28 AND (g.tgtype & 32) <> 0 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
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
  SELECT string_agg(con.conname, ', ') INTO bad FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f' AND n.nspname = cfg.app_schema AND con.confrelid IN (SELECT rel FROM scoped_table) AND con.conrelid NOT IN (SELECT rel FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('FK from an unscoped table into a scoped one: %s', bad)); END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i JOIN scoped_table t ON t.rel = i.inhparent WHERE i.inhrelid NOT IN (SELECT rel FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unregistered partitions: %s', bad)); END IF;
END $$;
CREATE FUNCTION on_ddl_end() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
  IF current_setting(current_schema() || '.installing', true) IS DISTINCT FROM 'on'
     AND EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() c WHERE c.schema_name = current_schema()) THEN
    PERFORM violation(format('DDL on guard schema %s outside the loom installer', current_schema()));
  END IF;
  IF hatch_open() THEN RETURN; END IF;
  PERFORM assert_scoped_schema();
END $$;
CREATE FUNCTION on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE bad text;
BEGIN
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unprotect before dropping a registered table: %s', bad)); END IF;
END $$;
CREATE FUNCTION set_password_verifier(role name, verifier text) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE cfg config%ROWTYPE;
BEGIN
  SELECT * INTO cfg FROM config;
  IF verifier !~ '^SCRAM-SHA-256\$[0-9]+:[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+$' THEN
    PERFORM reject(format('password for %s must be a SCRAM-SHA-256 verifier', role)); END IF;
  IF NOT role = ANY (cfg.login_roles) THEN
    PERFORM refuse(format('%s is not a login user of schema %s', role, cfg.app_schema)); END IF;
  EXECUTE format('ALTER ROLE %I PASSWORD %L', role, verifier);
END $$;
CREATE FUNCTION configure(doc jsonb) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  cfg config%ROWTYPE; guard name := current_schema(); app name := doc->>'app_schema';
  owner_r name := doc->>'owner'; migrator_r name := doc->>'migrator';
  readers_r name := doc->>'readers'; writers_r name := doc->>'writers';
  spec record; r pg_roles%ROWTYPE; bypass name[];
BEGIN
  SELECT * INTO cfg FROM config;
  IF FOUND AND (cfg.app_schema, cfg.owner_role, cfg.migrator_role, cfg.readers_role, cfg.writers_role)
               IS DISTINCT FROM (app, owner_r, migrator_r, readers_r, writers_r) THEN
    PERFORM refuse(format('guard %s is configured for another schema or other roles', guard)); END IF;
  FOR spec IN
    SELECT owner_r AS name_, false AS login_, false AS bypass_, true AS inherit_
    UNION ALL SELECT migrator_r, true, false, false
    UNION ALL SELECT readers_r, false, false, true
    UNION ALL SELECT writers_r, false, false, true
    UNION ALL SELECT u->>'name', (u->>'login')::boolean, u->>'access' = 'bypass', u->>'access' <> 'bypass'
      FROM jsonb_array_elements(doc->'users') u
  LOOP
    IF spec.name_ LIKE 'pg\_%' OR spec.name_ LIKE 'rds\_%' THEN
      PERFORM refuse(format('role %s uses a reserved prefix', spec.name_)); END IF;
    SELECT * INTO r FROM pg_roles WHERE rolname = spec.name_;
    IF NOT FOUND THEN
      EXECUTE format('CREATE ROLE %I %s %s %s', spec.name_,
        CASE WHEN spec.login_ THEN 'LOGIN' ELSE 'NOLOGIN' END,
        CASE WHEN spec.bypass_ THEN 'BYPASSRLS' ELSE 'NOBYPASSRLS' END,
        CASE WHEN spec.inherit_ THEN 'INHERIT' ELSE 'NOINHERIT' END);
    ELSIF r.rolcanlogin <> spec.login_ OR r.rolbypassrls <> spec.bypass_ OR r.rolinherit <> spec.inherit_
          OR r.rolsuper OR r.rolcreaterole OR r.rolcreatedb OR r.rolreplication THEN
      PERFORM refuse(format('role %s already exists with different attributes', spec.name_));
    END IF;
  END LOOP;
  EXECUTE format('GRANT %I TO %I', owner_r, migrator_r);
  EXECUTE format('ALTER ROLE %I SET role = %I', migrator_r, owner_r);
  EXECUTE format('ALTER ROLE %I SET search_path = %I, %I', owner_r, app, guard);
  EXECUTE format('ALTER ROLE %I SET search_path = %I, %I', migrator_r, app, guard);
  FOR spec IN SELECT u->>'name' AS name_, u->>'access' AS access_, (u->>'login')::boolean AS login_ FROM jsonb_array_elements(doc->'users') u LOOP
    IF spec.access_ = 'read' THEN EXECUTE format('GRANT %I TO %I', readers_r, spec.name_);
    ELSIF spec.access_ = 'write' THEN EXECUTE format('GRANT %I, %I TO %I', readers_r, writers_r, spec.name_);
    END IF;
    IF spec.login_ THEN EXECUTE format('ALTER ROLE %I SET search_path = %I', spec.name_, app); END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = app) THEN
    IF (SELECT rolname FROM pg_roles WHERE oid = (SELECT nspowner FROM pg_namespace WHERE nspname = app)) IS DISTINCT FROM owner_r THEN
      PERFORM refuse(format('schema %s exists and is not owned by %s; the bootstrap never adopts it', app, owner_r)); END IF;
  ELSE
    EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', app, owner_r);
  END IF;
  IF coalesce((doc->>'revoke_public')::boolean, false) THEN EXECUTE 'REVOKE ALL ON SCHEMA public FROM PUBLIC'; END IF;
  EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC', app);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I, %I', app, readers_r, writers_r);
  bypass := ARRAY(SELECT u->>'name' FROM jsonb_array_elements(doc->'users') u WHERE u->>'access' = 'bypass');
  FOR spec IN SELECT unnest(bypass) AS name_ LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', app, spec.name_);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', owner_r, app, spec.name_);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE ON SEQUENCES TO %I', owner_r, app, spec.name_);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I', app, spec.name_);
    EXECUTE format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA %I TO %I', app, spec.name_);
    IF to_regclass(format('%I.%I', app, doc->>'version_table')) IS NOT NULL THEN
      EXECUTE format('REVOKE ALL ON %I.%I FROM %I', app, doc->>'version_table', spec.name_);
      EXECUTE format('GRANT SELECT ON %I.%I TO %I', app, doc->>'version_table', spec.name_);
    END IF;
  END LOOP;
  INSERT INTO config VALUES (true, app, owner_r, migrator_r, readers_r, writers_r, bypass,
      ARRAY[migrator_r] || ARRAY(SELECT u->>'name' FROM jsonb_array_elements(doc->'users') u WHERE (u->>'login')::boolean),
      doc->>'version_table', doc->>'data_version_table')
    ON CONFLICT (singleton) DO UPDATE SET bypass_roles = EXCLUDED.bypass_roles, login_roles = EXCLUDED.login_roles,
      version_table = EXCLUDED.version_table, data_version_table = EXCLUDED.data_version_table;
  EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', guard, owner_r);
  EXECUTE format('GRANT SELECT ON %I.config, %I.scoped_table, %I.scoped_policy TO %I', guard, guard, guard, owner_r);
  FOR spec IN SELECT p.oid::regprocedure AS name_ FROM pg_proc p WHERE p.pronamespace = guard::regnamespace AND p.proname = ANY (owner_functions()) LOOP
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', spec.name_, owner_r);
  END LOOP;
  EXECUTE format('DROP EVENT TRIGGER IF EXISTS %I', guard || '_ddl');
  EXECUTE format('CREATE EVENT TRIGGER %I ON ddl_command_end EXECUTE FUNCTION %I.on_ddl_end()', guard || '_ddl', guard);
  EXECUTE format('ALTER EVENT TRIGGER %I ENABLE ALWAYS', guard || '_ddl');
  EXECUTE format('DROP EVENT TRIGGER IF EXISTS %I', guard || '_drop');
  EXECUTE format('CREATE EVENT TRIGGER %I ON sql_drop EXECUTE FUNCTION %I.on_sql_drop()', guard || '_drop', guard);
  EXECUTE format('ALTER EVENT TRIGGER %I ENABLE ALWAYS', guard || '_drop');
END $$;
DO $$ BEGIN EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', current_schema()); END $$;
