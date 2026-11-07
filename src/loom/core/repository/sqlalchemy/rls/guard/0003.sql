ALTER TABLE config ADD COLUMN auditor_role name, ADD COLUMN retention_months int CHECK (retention_months >= 1);
CREATE TABLE audited_table (rel regclass PRIMARY KEY, excluded text[] NOT NULL);
CREATE TABLE audit_partition (rel regclass PRIMARY KEY, lower_bound timestamptz NOT NULL, upper_bound timestamptz NOT NULL);
CREATE OR REPLACE FUNCTION owner_functions() RETURNS name[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['protect_scoped_table', 'unprotect_scoped_table', 'open_hatch', 'assert_scoped_schema',
               'grant_table', 'grant_privileges', 'check_privileges', 'prepare_version_tables', 'limit_version_table',
               'registered_tables', 'guard_schema', 'app_schema', 'owner_role', 'group_role', 'users_of',
               'tagged', 'reject', 'refuse', 'is_member', 'commands', 'authorize',
               'create_range_partition', 'detach_partition',
               'audit_log_of', 'audit_log_rel', 'audit_trigger_source', 'prepare_audit_log', 'protect_audit_log',
               'protect_audited_table', 'unprotect_audited_table', 'create_audit_partition',
               'protect_audit_partition', 'drop_expired_audit_partition', 'refuse_audit_log_change',
               'require_audit_log']::name[] $$;
CREATE FUNCTION audit_log_of(app name) RETURNS regclass LANGUAGE sql STABLE AS $$
  SELECT to_regclass(format('%I.audit_log', app)) $$;
CREATE FUNCTION audit_log_rel() RETURNS regclass LANGUAGE sql STABLE AS $$
  SELECT audit_log_of(app_schema()) $$;
CREATE FUNCTION grant_schema_usage(app name, grantee name) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', app, grantee);
END $$;
CREATE FUNCTION audit_trigger_source() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT format($src$
DECLARE
  v_old jsonb;
  v_new jsonb;
  v_row jsonb;
  v_root oid := TG_RELID;
  v_excluded text[];
  v_changed jsonb;
  v_key jsonb;
  v_scopes jsonb;
  v_elevated text[];
  v_schema name := TG_TABLE_SCHEMA;
  v_table name := TG_TABLE_NAME;
BEGIN
  IF TG_OP <> 'INSERT' THEN v_old := to_jsonb(OLD); END IF;
  IF TG_OP <> 'DELETE' THEN v_new := to_jsonb(NEW); END IF;
  IF TG_OP = 'UPDATE' AND v_old = v_new THEN RETURN NULL; END IF;
  v_row := coalesce(v_new, v_old);
  IF TG_LEVEL = 'ROW' AND (SELECT c.relispartition FROM pg_class c WHERE c.oid = TG_RELID) THEN
    v_root := pg_partition_root(TG_RELID);
    SELECT n.nspname, c.relname INTO v_schema, v_table FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = v_root;
  END IF;
  SELECT a.excluded INTO v_excluded FROM %1$I.audited_table a WHERE a.rel = v_root;
  IF NOT FOUND THEN
    RAISE EXCEPTION USING ERRCODE = 'LG002', MESSAGE = 'loom_guard: ' || TG_RELID::regclass::text || ' is not a registered audited table';
  END IF;
  SELECT coalesce(jsonb_object_agg(k, CASE
           WHEN k = ANY (v_excluded) THEN jsonb_build_object('redacted', true)
           WHEN TG_OP = 'INSERT' THEN jsonb_build_object('new', v_new -> k)
           WHEN TG_OP = 'DELETE' THEN jsonb_build_object('old', v_old -> k)
           ELSE jsonb_build_object('old', v_old -> k, 'new', v_new -> k) END), '{}')
    INTO v_changed
    FROM jsonb_object_keys(v_row) k
    WHERE TG_OP <> 'UPDATE' OR (v_old -> k) IS DISTINCT FROM (v_new -> k);
  SELECT coalesce(jsonb_object_agg(a.attname, v_row -> a.attname::text), '{}') INTO v_key
    FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
    WHERE i.indrelid = TG_RELID AND i.indisprimary;
  SELECT coalesce(jsonb_object_agg(s.scope_name, v_row -> s.col_name), '{}'),
         coalesce(array_agg(s.scope_name) FILTER (WHERE current_setting('loom.scope.' || s.scope_name || '.any', true) = 'on'), '{}')
    INTO v_scopes, v_elevated
    FROM (SELECT x ->> 'scope' AS scope_name, x ->> 'col' AS col_name
          FROM %1$I.scoped_table t, jsonb_array_elements(t.scopes) x WHERE t.rel = TG_RELID) s;
  INSERT INTO audit_log (at, txid, schema_name, table_name, row_key, op, changed, subject, mechanism, on_behalf_of,
                         elevated, scopes, trace_id, db_user, app_name, client_addr, backend_pid, origin)
  VALUES (now(), pg_current_xact_id(), v_schema, v_table, v_key, left(TG_OP, 1), v_changed,
          coalesce(current_setting('loom.actor.subject', true), ''),
          coalesce(current_setting('loom.actor.mechanism', true), ''),
          coalesce(current_setting('loom.actor.on_behalf_of', true), ''),
          v_elevated, v_scopes, coalesce(current_setting('loom.trace_id', true), ''),
          session_user, coalesce(current_setting('application_name', true), ''),
          inet_client_addr(), pg_backend_pid(), 'trigger');
  RETURN NULL;
END
$src$, guard_schema()) $$;
CREATE FUNCTION require_audit_log() RETURNS regclass LANGUAGE plpgsql STABLE AS $$
DECLARE log regclass := audit_log_rel();
BEGIN
  IF log IS NULL THEN PERFORM refuse('the audit log does not exist; prepare it first'); END IF;
  RETURN log;
END $$;
CREATE FUNCTION refuse_audit_log_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION USING ERRCODE = 'LG003',
    MESSAGE = 'loom_guard[' || TG_TABLE_SCHEMA || ']: the audit log is append-only; ' || TG_OP || ' on ' || TG_TABLE_NAME || ' is refused';
END $$;
CREATE FUNCTION apply_audit_acl(c config, tbl regclass) RETURNS void LANGUAGE plpgsql AS $$
DECLARE bypass name; seq regclass;
BEGIN
  EXECUTE format('REVOKE ALL ON %s FROM PUBLIC, %I, %I', tbl, c.readers_role, c.writers_role);
  FOR bypass IN SELECT u.name FROM users_of(c.users) u WHERE u.bypass LOOP
    EXECUTE format('REVOKE ALL ON %s FROM %I', tbl, bypass);
    FOR seq IN SELECT sq.oid::regclass FROM pg_depend d JOIN pg_class sq ON sq.oid = d.objid AND sq.relkind = 'S'
               WHERE d.refobjid = tbl AND d.deptype IN ('a', 'i') LOOP
      EXECUTE format('REVOKE ALL ON SEQUENCE %s FROM %I', seq, bypass);
    END LOOP;
  END LOOP;
  IF c.auditor_role IS NOT NULL THEN EXECUTE format('GRANT SELECT ON %s TO %I', tbl, c.auditor_role); END IF;
END $$;
CREATE FUNCTION restrict_audit_log(c config) RETURNS void LANGUAGE plpgsql AS $$
DECLARE log regclass := audit_log_of(c.app_schema); part regclass;
BEGIN
  IF log IS NULL THEN RETURN; END IF;
  PERFORM apply_audit_acl(c, log);
  FOR part IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = log LOOP
    PERFORM apply_audit_acl(c, part);
  END LOOP;
END $$;
CREATE FUNCTION make_immutable(tbl regclass) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = tbl AND tgname = 'loom_audit_log_immutable_row') THEN
    EXECUTE format('CREATE TRIGGER loom_audit_log_immutable_row BEFORE UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %I.refuse_audit_log_change()', tbl, guard_schema());
  END IF;
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_audit_log_immutable_row', tbl);
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = tbl AND tgname = 'loom_audit_log_immutable_truncate') THEN
    EXECUTE format('CREATE TRIGGER loom_audit_log_immutable_truncate BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION %I.refuse_audit_log_change()', tbl, guard_schema());
  END IF;
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_audit_log_immutable_truncate', tbl);
END $$;
CREATE FUNCTION prepare_audit_log() RETURNS void LANGUAGE plpgsql AS $$
DECLARE c config;
BEGIN
  SELECT * INTO c FROM config;
  IF NOT is_member(session_user, c.owner_role) THEN PERFORM refuse('only the owner may prepare the audit log'); END IF;
  IF c.auditor_role IS NULL THEN PERFORM refuse('the auditor role is not configured; run the bootstrap'); END IF;
  PERFORM open_hatch();
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I.audit_log ('
    'id bigint GENERATED ALWAYS AS IDENTITY, at timestamptz NOT NULL, txid xid8 NOT NULL, '
    'schema_name text NOT NULL, table_name text NOT NULL, row_key jsonb NOT NULL, '
    'op char(1) NOT NULL CHECK (op IN (%L, %L, %L)), changed jsonb NOT NULL, '
    'subject text NOT NULL DEFAULT %L, mechanism text NOT NULL DEFAULT %L, on_behalf_of text NOT NULL DEFAULT %L, '
    'elevated text[] NOT NULL DEFAULT %L, scopes jsonb NOT NULL DEFAULT %L, trace_id text NOT NULL DEFAULT %L, '
    'db_user name NOT NULL, app_name text NOT NULL DEFAULT %L, client_addr inet, backend_pid integer, '
    'origin text NOT NULL CHECK (origin IN (%L, %L)), PRIMARY KEY (at, id)) PARTITION BY RANGE (at)',
    c.app_schema, 'I', 'U', 'D', '', '', '', '{}', '{}', '', '', 'trigger', 'cdc');
  EXECUTE format('CREATE INDEX IF NOT EXISTS audit_log_row_idx ON %I.audit_log (schema_name, table_name, row_key)', c.app_schema);
  EXECUTE format('CREATE INDEX IF NOT EXISTS audit_log_trace_idx ON %I.audit_log (trace_id)', c.app_schema);
  EXECUTE format('CREATE INDEX IF NOT EXISTS audit_log_scopes_idx ON %I.audit_log USING gin (scopes)', c.app_schema);
  EXECUTE format('CREATE OR REPLACE FUNCTION %I.write_audit_row() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER '
                 'SET search_path = pg_catalog, %I, pg_temp AS %L', c.app_schema, c.app_schema, audit_trigger_source());
  EXECUTE format('REVOKE ALL ON FUNCTION %I.write_audit_row() FROM PUBLIC', c.app_schema);
  PERFORM protect_audit_log();
END $$;
CREATE FUNCTION protect_audit_log() RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE c config; log regclass := require_audit_log();
BEGIN
  SELECT * INTO c FROM config;
  PERFORM authorize(log, 'protect');
  PERFORM enter_hatch();
  PERFORM apply_audit_acl(c, log);
  PERFORM make_immutable(log);
  PERFORM leave_hatch();
END $$;
CREATE FUNCTION protect_audited_table(tbl regclass, excluded text[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE col text; keys text[];
BEGIN
  PERFORM authorize(tbl, 'audit');
  PERFORM require_audit_log();
  IF (SELECT relispartition FROM pg_class WHERE oid = tbl) THEN
    PERFORM reject(format('%s is a partition; audit its partitioned table', tbl)); END IF;
  FOREACH col IN ARRAY excluded LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = col AND attnum > 0 AND NOT attisdropped) THEN
      PERFORM reject(format('excluded column %s does not exist on %s', col, tbl)); END IF;
  END LOOP;
  SELECT array_agg(a.attname::text) INTO keys FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
    WHERE i.indrelid = tbl AND i.indisprimary;
  IF keys IS NULL THEN PERFORM reject(format('audited table %s needs a primary key', tbl)); END IF;
  IF keys && excluded THEN PERFORM reject(format('%s: a primary key column cannot be excluded from the audit log', tbl)); END IF;
  IF EXISTS (SELECT 1 FROM audited_table WHERE rel = tbl) THEN
    UPDATE audited_table SET excluded = protect_audited_table.excluded WHERE rel = tbl;
    RETURN;
  END IF;
  PERFORM enter_hatch();
  EXECUTE format('CREATE TRIGGER loom_audit_record AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %I.write_audit_row()', tbl, app_schema());
  EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER loom_audit_record', tbl);
  INSERT INTO audited_table VALUES (tbl, excluded);
  PERFORM leave_hatch();
END $$;
CREATE FUNCTION unprotect_audited_table(tbl regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  PERFORM authorize(tbl, 'stop auditing');
  IF NOT EXISTS (SELECT 1 FROM audited_table WHERE rel = tbl) THEN
    PERFORM refuse(format('%s is not an audited table', tbl)); END IF;
  PERFORM enter_hatch();
  EXECUTE format('DROP TRIGGER IF EXISTS loom_audit_record ON %s', tbl);
  DELETE FROM audited_table WHERE rel = tbl;
  PERFORM leave_hatch();
END $$;
CREATE FUNCTION create_audit_partition(partition_name text, lower_bound text, upper_bound text) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE log regclass := require_audit_log(); qualified text := format('%I.%I', app_schema(), partition_name); part regclass;
BEGIN
  PERFORM authorize(log, 'partition');
  IF coalesce(octet_length(partition_name), 0) NOT BETWEEN 1 AND 63 THEN
    PERFORM reject(format('partition name %s is empty or longer than 63 bytes', partition_name)); END IF;
  part := to_regclass(qualified);
  IF part IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = part AND inhparent = log)
       AND EXISTS (SELECT 1 FROM audit_partition WHERE rel = part) THEN RETURN false; END IF;
    PERFORM refuse(format('%s exists and is not a registered partition of the audit log', part));
  END IF;
  PERFORM open_hatch();
  EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)', qualified, log, lower_bound, upper_bound);
  PERFORM protect_audit_partition(to_regclass(qualified), lower_bound::timestamptz, upper_bound::timestamptz);
  RETURN true;
END $$;
CREATE FUNCTION protect_audit_partition(part regclass, lower_bound timestamptz, upper_bound timestamptz) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE c config; log regclass := audit_log_rel();
BEGIN
  SELECT * INTO c FROM config;
  PERFORM authorize(log, 'partition');
  IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = part AND inhparent = log) THEN
    PERFORM refuse(format('%s is not a partition of the audit log', part)); END IF;
  PERFORM enter_hatch();
  PERFORM apply_audit_acl(c, part);
  PERFORM make_immutable(part);
  INSERT INTO audit_partition VALUES (part, lower_bound, upper_bound) ON CONFLICT (rel) DO NOTHING;
  PERFORM leave_hatch();
END $$;
CREATE FUNCTION drop_expired_audit_partition(partition_name text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE log regclass := audit_log_rel(); part regclass; registered audit_partition; months int;
BEGIN
  IF log IS NULL THEN PERFORM refuse('the audit log does not exist'); END IF;
  PERFORM authorize(log, 'drop partitions of');
  part := to_regclass(format('%I.%I', app_schema(), partition_name));
  SELECT * INTO registered FROM audit_partition WHERE rel = part;
  IF part IS NULL OR NOT FOUND THEN
    PERFORM refuse(format('%s is not a registered partition of the audit log', partition_name)); END IF;
  SELECT retention_months INTO months FROM config;
  IF months IS NULL THEN
    PERFORM refuse('no audit retention is configured; the audit log is never dropped without one'); END IF;
  IF registered.upper_bound > date_trunc('month', now()) - make_interval(months => months) THEN
    PERFORM refuse(format('%s is within the %s-month retention', partition_name, months)); END IF;
  PERFORM enter_hatch();
  DELETE FROM audit_partition WHERE rel = part;
  EXECUTE format('DROP TABLE %s', part);
  PERFORM leave_hatch();
  RETURN true;
END $$;
CREATE FUNCTION audit_trigger_allowed(tbl oid, trigger_name name) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT (trigger_name = 'loom_audit_record' AND coalesce(pg_partition_root(tbl), tbl) IN (SELECT rel::oid FROM audited_table))
      OR (trigger_name LIKE 'loom\_audit\_log\_immutable\_%'
          AND coalesce(pg_partition_root(tbl), tbl) = audit_log_rel()::oid) $$;
CREATE FUNCTION assert_audited_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  bad text; cfg config; log regclass := audit_log_rel(); writer regprocedure; refuser regprocedure;
  record_trigger name := 'loom_audit_record';
  row_trigger name := 'loom_audit_log_immutable_row';
  truncate_trigger name := 'loom_audit_log_immutable_truncate';
BEGIN
  SELECT * INTO cfg FROM config;
  writer := to_regprocedure(format('%I.write_audit_row()', cfg.app_schema));
  refuser := to_regprocedure(quote_ident(guard_schema()) || '.refuse_audit_log_change()');
  IF log IS NULL THEN
    IF EXISTS (SELECT 1 FROM audited_table) THEN PERFORM violation('audited tables without an audit log'); END IF;
    RETURN;
  END IF;
  IF writer IS NULL OR NOT EXISTS (
       SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
       WHERE p.oid = writer AND p.prosecdef AND r.rolname = cfg.owner_role AND p.prosrc = audit_trigger_source()
         AND array_length(p.proconfig, 1) = 1 AND p.proconfig[1] LIKE 'search\_path=pg\_catalog, %') THEN
    PERFORM violation(format('the audit writer %s.write_audit_row differs from the released one', cfg.app_schema)); END IF;
  SELECT string_agg(a.rel::text, ', ') INTO bad FROM audited_table a WHERE NOT EXISTS (
    SELECT 1 FROM pg_trigger g WHERE g.tgrelid = a.rel AND g.tgname = record_trigger AND g.tgenabled = 'A' AND g.tgfoid = writer
      AND (g.tgtype & 1) <> 0 AND (g.tgtype & 2) = 0 AND (g.tgtype & 28) = 28 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit trigger missing, disabled, wrong function or wrong events: %s', bad)); END IF;
  SELECT string_agg(t.relid::text, ', ') INTO bad FROM audited_table a, pg_partition_tree(a.rel) t
    WHERE t.relid <> a.rel::oid AND NOT EXISTS (
      SELECT 1 FROM pg_trigger g WHERE g.tgrelid = t.relid AND g.tgname = record_trigger AND g.tgenabled IN ('A', 'O') AND g.tgfoid = writer);
  IF bad IS NOT NULL THEN PERFORM violation(format('partitions of audited tables without the audit trigger: %s', bad)); END IF;
  SELECT string_agg(x.r::regclass::text, ', ') INTO bad
    FROM (SELECT log::oid UNION ALL SELECT inhrelid FROM pg_inherits WHERE inhparent = log) AS x(r)
    WHERE NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.r AND g.tgname = row_trigger AND g.tgenabled = 'A'
                        AND g.tgfoid = refuser AND (g.tgtype & 1) <> 0 AND (g.tgtype & 2) <> 0 AND (g.tgtype & 24) = 24)
       OR NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.r AND g.tgname = truncate_trigger AND g.tgenabled = 'A'
                        AND g.tgfoid = refuser AND (g.tgtype & 1) = 0 AND (g.tgtype & 32) <> 0);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log immutability triggers missing or disabled: %s', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM (SELECT log::oid UNION ALL SELECT inhrelid FROM pg_inherits WHERE inhparent = log) AS x(r)
    JOIN pg_class c ON c.oid = x.r, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT (g.rolname IS NOT DISTINCT FROM cfg.auditor_role AND a.privilege_type = 'SELECT');
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log grant outside {auditor: SELECT}: %s', bad)); END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i
    WHERE i.inhparent = log AND i.inhrelid NOT IN (SELECT rel::oid FROM audit_partition);
  IF bad IS NOT NULL THEN PERFORM violation(format('unregistered audit log partitions: %s', bad)); END IF;
  SELECT string_agg(p.rel::text, ', ') INTO bad FROM audit_partition p
    WHERE NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.rel AND i.inhparent = log);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log partitions detached: %s', bad)); END IF;
END $$;
CREATE OR REPLACE FUNCTION assert_scoped_schema() RETURNS void
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
           OR EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = c.oid AND g.tgname LIKE 'loom\_%' AND NOT audit_trigger_allowed(c.oid, g.tgname)));
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
  PERFORM assert_audited_schema();
END $$;
CREATE OR REPLACE FUNCTION on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE bad text;
BEGIN
  PERFORM forbid_guard_ddl(EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() d
                                   WHERE d.schema_name = guard_schema() OR d.object_identity = quote_ident(guard_schema())));
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unprotect before dropping a registered table: %s', bad)); END IF;
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table')
      AND ((d.schema_name = app_schema() AND d.object_name = 'audit_log') OR d.objid IN (SELECT rel::oid FROM audit_partition));
  IF bad IS NOT NULL THEN PERFORM violation(format('the audit log is dropped only by drop_expired_audit_partition: %s', bad)); END IF;
  DELETE FROM audited_table WHERE rel::oid IN (SELECT d.objid FROM pg_event_trigger_dropped_objects() d
                                               WHERE d.classid = 'pg_class'::regclass);
  IF hatch_open() OR NOT EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() d WHERE d.schema_name = app_schema()) THEN
    RETURN; END IF;
  PERFORM assert_scoped_schema();
END $$;
CREATE OR REPLACE FUNCTION role_specs(c config) RETURNS TABLE (name name, login boolean, bypass boolean, inherit boolean)
  LANGUAGE sql IMMUTABLE AS $$
  VALUES (c.owner_role, false, false, true), (c.migrator_role, true, false, false),
         (c.readers_role, false, false, true), (c.writers_role, false, false, true)
  UNION ALL SELECT u.name, u.login, u.bypass, NOT u.bypass FROM users_of(c.users) u
  UNION ALL SELECT c.auditor_role, false, false, true WHERE c.auditor_role IS NOT NULL $$;
CREATE OR REPLACE FUNCTION unexpected_memberships(c config) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(format('%s in %s%s', m.rolname, r.rolname, CASE WHEN am.admin_option THEN ' with admin option' ELSE '' END), ', ')
  FROM pg_auth_members am JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles m ON m.oid = am.member
  WHERE (r.rolname IN (SELECT s.name FROM role_specs(c) s) OR m.rolname IN (SELECT s.name FROM role_specs(c) s))
    AND NOT m.rolsuper AND m.rolname <> c.installer
    AND NOT (r.rolname = c.auditor_role AND NOT am.admin_option AND m.rolname NOT IN (SELECT s.name FROM role_specs(c) s))
    AND (am.admin_option OR (r.rolname, m.rolname) NOT IN (SELECT e.role_name, e.member FROM expected_members(c) e)) $$;
CREATE OR REPLACE FUNCTION grant_bypass(c config, role_name name) RETURNS void LANGUAGE plpgsql AS $$
DECLARE log regclass := audit_log_of(c.app_schema); rel regclass;
BEGIN
  PERFORM grant_schema_usage(c.app_schema, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', c.owner_role, c.app_schema, role_name);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE ON SEQUENCES TO %I', c.owner_role, c.app_schema, role_name);
  FOR rel IN SELECT k.oid::regclass FROM pg_class k
             WHERE k.relnamespace = c.app_schema::regnamespace AND k.relkind IN ('r', 'p', 'v', 'm', 'f')
               AND coalesce(pg_partition_root(k.oid), k.oid) IS DISTINCT FROM log::oid ORDER BY k.oid LOOP
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO %I', rel, role_name);
  END LOOP;
  FOR rel IN SELECT k.oid::regclass FROM pg_class k
             WHERE k.relnamespace = c.app_schema::regnamespace AND k.relkind = 'S'
               AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = k.oid AND d.deptype IN ('a', 'i')
                               AND coalesce(pg_partition_root(d.refobjid), d.refobjid) = log::oid) ORDER BY k.oid LOOP
    EXECUTE format('GRANT USAGE ON SEQUENCE %s TO %I', rel, role_name);
  END LOOP;
END $$;
CREATE OR REPLACE FUNCTION secure_guard(c config) RETURNS void LANGUAGE plpgsql AS $$
DECLARE guard name := guard_schema(); fn regprocedure; t record;
BEGIN
  EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', guard);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', guard, c.owner_role);
  EXECUTE format('GRANT SELECT ON config, scoped_table, scoped_policy, audited_table, audit_partition TO %I', c.owner_role);
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
CREATE OR REPLACE FUNCTION configure(doc jsonb) RETURNS void LANGUAGE plpgsql AS $$
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
  IF configured AND to_jsonb(cfg) - 'users' - 'auditor_role' - 'retention_months'
                    IS DISTINCT FROM to_jsonb(wanted) - 'users' - 'auditor_role' - 'retention_months' THEN
    PERFORM refuse(format('guard %s is configured for another schema, other roles, other version tables or another installer', guard_schema())); END IF;
  IF configured AND cfg.auditor_role IS NOT NULL AND wanted.auditor_role IS DISTINCT FROM cfg.auditor_role THEN
    PERFORM refuse(format('guard %s already has the auditor role %s; it never changes', guard_schema(), cfg.auditor_role)); END IF;
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
  IF wanted.auditor_role IS NOT NULL THEN
    PERFORM grant_schema_usage(wanted.app_schema, wanted.auditor_role); END IF;
  FOR spec IN SELECT u.name FROM users_of(wanted.users) u WHERE u.bypass LOOP
    PERFORM grant_bypass(wanted, spec.name);
  END LOOP;
  PERFORM restrict_audit_log(wanted);
  PERFORM limit_version_table(wanted);
  INSERT INTO config SELECT (wanted).* ON CONFLICT (singleton) DO UPDATE
    SET users = EXCLUDED.users, auditor_role = EXCLUDED.auditor_role, retention_months = EXCLUDED.retention_months;
  bad := unexpected_memberships(wanted);
  IF bad IS NOT NULL THEN PERFORM refuse(format('unexpected role memberships: %s', bad)); END IF;
  PERFORM secure_guard(wanted);
END $$;
DO $$
DECLARE fn regprocedure;
BEGIN
  FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p WHERE p.pronamespace = current_schema()::regnamespace AND p.proconfig IS NULL LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = pg_catalog, %I, pg_temp', fn, current_schema());
  END LOOP;
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', current_schema());
END $$;
