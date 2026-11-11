ALTER TABLE config ADD COLUMN audit_reading boolean NOT NULL DEFAULT false,
                   ADD COLUMN boundary_scope text,
                   ADD COLUMN boundary_type text,
                   ADD CONSTRAINT boundary_pair CHECK ((boundary_scope IS NULL) = (boundary_type IS NULL));
CREATE OR REPLACE FUNCTION owner_functions() RETURNS name[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['protect_scoped_table', 'unprotect_scoped_table', 'open_hatch', 'assert_scoped_schema',
               'grant_table', 'grant_privileges', 'check_privileges', 'prepare_version_tables', 'limit_version_table',
               'registered_tables', 'guard_schema', 'app_schema', 'owner_role', 'group_role', 'users_of',
               'tagged', 'reject', 'refuse', 'is_member', 'commands', 'authorize',
               'create_range_partition', 'detach_partition',
               'audit_log_of', 'audit_log_rel', 'audit_trigger_source', 'prepare_audit_log', 'protect_audit_log',
               'protect_audited_table', 'unprotect_audited_table', 'create_audit_partition',
               'ensure_audit_partitions', 'utc_month',
               'protect_audit_partition', 'drop_expired_audit_partition', 'refuse_audit_log_change',
               'require_audit_log', 'audit_reader_source', 'apply_audit_reading', 'unbounded_tables',
               'scope_of', 'duplicate_scopes', 'normal_scope', 'scope_set', 'tree_mismatch']::name[] $$;
CREATE FUNCTION scope_of(e jsonb) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT e->>'scope' $$;
CREATE FUNCTION duplicate_scopes(scopes jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT count(DISTINCT lower(scope_of(x))) <> count(*) FROM jsonb_array_elements(scopes) x $$;
CREATE FUNCTION normal_scope(e jsonb) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT e || jsonb_build_object('on', coalesce(e->>'on', 'both'), k.name, coalesce((e->>k.name)::boolean, false))
  FROM (SELECT 'elevable'::text AS name) k $$;
CREATE FUNCTION scope_set(scopes jsonb) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(jsonb_agg(DISTINCT normal_scope(x)), '[]') FROM jsonb_array_elements(scopes) x $$;
CREATE FUNCTION tree_mismatch(tbl regclass, scopes jsonb) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(t.rel::text, ', ' ORDER BY t.rel::text)
  FROM pg_partition_tree(coalesce(pg_partition_root(tbl), tbl)) p JOIN scoped_table t ON t.rel = p.relid
  WHERE t.rel <> tree_mismatch.tbl AND scope_set(t.scopes) IS DISTINCT FROM scope_set(tree_mismatch.scopes) $$;
CREATE OR REPLACE FUNCTION protect_scoped_table(tbl regclass, scopes jsonb, table_privileges text[]) RETURNS void
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
    scope_name := scope_of(e);
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = e->>'col' AND attnum > 0 AND NOT attisdropped) THEN
      PERFORM reject(format('scope column %s does not exist on %s', e->>'col', tbl)); END IF;
    IF NOT reach(e) = ANY (reaches()) THEN
      PERFORM reject(format('scope reach %s is not read/write/both', e->>'on')); END IF;
    IF coalesce(scope_name, '') !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
      PERFORM reject(format('scope name %s is not an identifier', scope_name)); END IF;
  END LOOP;
  IF duplicate_scopes(scopes) THEN
    PERFORM reject(format('scope names of %s must be unique', tbl)); END IF;
  PERFORM pg_advisory_xact_lock(coalesce(pg_partition_root(tbl), tbl)::oid::bigint);
  IF tree_mismatch(tbl, scopes) IS NOT NULL THEN
    PERFORM reject(format('%s must have the scopes of its partition tree; they differ on: %s', tbl, tree_mismatch(tbl, scopes))); END IF;
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
CREATE OR REPLACE FUNCTION assert_scoped_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE bad text; cfg config; app oid; guard oid;
BEGIN
  SELECT * INTO cfg FROM config;
  SELECT oid INTO app FROM pg_namespace WHERE nspname = cfg.app_schema;
  SELECT oid INTO guard FROM pg_namespace WHERE nspname = guard_schema();
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t JOIN pg_class c ON c.oid = t.rel WHERE NOT (c.relrowsecurity AND c.relforcerowsecurity);
  IF bad IS NOT NULL THEN PERFORM violation(format('registered tables without forced RLS: %s', bad)); END IF;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t WHERE duplicate_scopes(t.scopes);
  IF bad IS NOT NULL THEN PERFORM violation(format('registered tables with duplicate scope names: %s', bad)); END IF;
  SELECT string_agg(t.rel::text, ', ') INTO bad FROM scoped_table t WHERE tree_mismatch(t.rel, t.scopes) IS NOT NULL;
  IF bad IS NOT NULL THEN PERFORM violation(format('partitions registered with scopes other than their tree''s: %s', bad)); END IF;
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
  -- A parent reads the rows of its children with its own policies: every ancestor of a registered table is registered.
  SELECT string_agg(i.inhrelid::regclass::text || ' under ' || i.inhparent::regclass::text, ', ') INTO bad FROM pg_inherits i
    WHERE i.inhrelid IN (SELECT rel FROM scoped_table) AND i.inhparent NOT IN (SELECT rel FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('registered tables under an unregistered parent: %s', bad)); END IF;
  PERFORM assert_audited_schema();
END $$;
CREATE FUNCTION audit_reader_source() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT format($src$
#variable_conflict use_column
DECLARE
  c_span constant interval := '31 days';
  c_elevable constant text := 'elevable';
  v_set jsonb;
  v_cfg record;
  v_table record;
  v_scope record;
  v_from timestamptz;
  v_to timestamptz;
  v_value jsonb;
  v_prefilter jsonb;
  v_unscoped boolean := false;
  v_specs jsonb := '[]';
  v_keys text[];
  v_allcols text[];
  v_expected jsonb;
  v_cols jsonb;
  v_mappable boolean;
  v_complete boolean;
  v_bounded boolean;
  v_unique boolean;
  v_invalid boolean;
  v_sql text;
BEGIN
  IF p_all IS NULL OR p_all = (p_subjects IS NOT NULL)
     OR p_cap IS NULL OR p_cap < 1 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 101
     OR p_ascending IS NULL OR (p_after_at IS NULL) <> (p_after_id IS NULL)
     OR p_to - p_from > c_span THEN
    v_invalid := true;
  ELSIF NOT p_all AND cardinality(p_subjects) > p_cap THEN
    RAISE EXCEPTION USING ERRCODE = 'LG004', MESSAGE = 'loom_guard: actor set over the cap';
  ELSE
    v_invalid := EXISTS (SELECT 1 FROM unnest(p_subjects) AS s(v) WHERE coalesce(s.v, '') = '');
  END IF;
  IF v_invalid THEN
    RAISE EXCEPTION USING ERRCODE = 'LG005', MESSAGE = 'loom_guard: invalid reader arguments';
  END IF;
  v_from := coalesce(p_from, p_to - c_span, now() - c_span);
  v_to := coalesce(p_to, p_from + c_span);
  SELECT g.boundary_scope, g.boundary_type INTO v_cfg FROM %1$I.config g;
  IF NOT FOUND THEN RETURN; END IF;
  IF v_cfg.boundary_scope IS NULL THEN
    IF EXISTS (SELECT 1 FROM %1$I.scoped_table) THEN RETURN; END IF;
    v_unscoped := true;
  ELSE
    BEGIN
      EXECUTE format('SELECT to_jsonb(CAST(NULLIF($1, '''') AS %%s))', v_cfg.boundary_type)
        INTO v_value USING current_setting('loom.scope.' || v_cfg.boundary_scope, true);
    EXCEPTION WHEN data_exception THEN v_value := NULL;
    END;
    IF v_value IS NULL THEN RETURN; END IF;
    IF jsonb_typeof(v_value) IN ('string', 'number', 'boolean') THEN
      v_prefilter := jsonb_build_object(v_cfg.boundary_scope, v_value);
    END IF;
  END IF;
  FOR v_table IN SELECT n.nspname::text AS tbl_schema, c.relname::text AS tbl_name, t.rel AS tbl_rel, t.scopes AS tbl_scopes
                 FROM %1$I.scoped_table t JOIN pg_class c ON c.oid = t.rel JOIN pg_namespace n ON n.oid = c.relnamespace LOOP
    v_keys := '{}'; v_allcols := '{}'; v_expected := '{}'; v_cols := '{}'; v_mappable := true; v_complete := true; v_bounded := false; v_unique := true;
    FOR v_scope IN SELECT x ->> 'scope' AS scope_name, x ->> 'col' AS col_name,
                          coalesce(x ->> 'on', 'both') <> 'write' AS for_reads, format_type(a.atttypid, NULL) AS col_type,
                          coalesce(x ->> 'on', 'both') = 'both' AND NOT coalesce((x ->> c_elevable)::boolean, false) AS is_bound
                   FROM jsonb_array_elements(v_table.tbl_scopes) x
                   LEFT JOIN pg_attribute a ON a.attrelid = v_table.tbl_rel AND a.attname = x ->> 'col'
                                           AND a.attnum > 0 AND NOT a.attisdropped LOOP
      v_unique := v_unique AND v_scope.scope_name IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM unnest(v_keys) AS u(k) WHERE lower(u.k) = lower(v_scope.scope_name));
      v_keys := v_keys || v_scope.scope_name;
      v_allcols := v_allcols || v_scope.col_name;
      v_mappable := v_mappable AND v_scope.col_type IS NOT NULL;
      v_bounded := v_bounded OR (v_scope.is_bound AND v_scope.scope_name = v_cfg.boundary_scope
                                 AND v_scope.col_type = v_cfg.boundary_type);
      CONTINUE WHEN NOT v_scope.for_reads;
      v_value := NULL;
      IF v_scope.col_type IS NOT NULL THEN
        BEGIN
          EXECUTE format('SELECT to_jsonb(CAST(NULLIF($1, '''') AS %%s))', v_scope.col_type)
            INTO v_value USING current_setting('loom.scope.' || v_scope.scope_name, true);
        EXCEPTION WHEN data_exception THEN v_value := NULL;
        END;
      END IF;
      v_complete := v_complete AND v_value IS NOT NULL;
      EXIT WHEN NOT v_complete;
      v_expected := v_expected || jsonb_build_object(v_scope.scope_name, v_value);
      v_cols := v_cols || jsonb_build_object(v_scope.scope_name, v_scope.col_name);
    END LOOP;
    CONTINUE WHEN NOT v_complete OR NOT v_unique OR NOT (v_unscoped OR v_bounded);
    SELECT coalesce(jsonb_agg(DISTINCT x || jsonb_build_object('on', coalesce(x ->> 'on', 'both'), c_elevable,
                                                               coalesce((x ->> c_elevable)::boolean, false))), '[]')
      INTO v_set FROM jsonb_array_elements(v_table.tbl_scopes) x;
    CONTINUE WHEN EXISTS (
      SELECT 1 FROM pg_partition_tree(v_table.tbl_rel) p JOIN %1$I.scoped_table s ON s.rel = p.relid
      WHERE p.relid <> v_table.tbl_rel
        AND (SELECT coalesce(jsonb_agg(DISTINCT x || jsonb_build_object('on', coalesce(x ->> 'on', 'both'), c_elevable,
                                                                        coalesce((x ->> c_elevable)::boolean, false))), '[]')
             FROM jsonb_array_elements(s.scopes) x) IS DISTINCT FROM v_set);
    v_specs := v_specs || jsonb_build_array(jsonb_build_object(
      'relschema', v_table.tbl_schema, 'relname', v_table.tbl_name,
      'keys', to_jsonb(ARRAY(SELECT d.k FROM (SELECT DISTINCT u.k FROM unnest(v_keys) AS u(k)) d ORDER BY d.k COLLATE "C")),
      'expected', v_expected, 'cols', v_cols, 'allcols', to_jsonb(v_allcols),
      'columns', to_jsonb(ARRAY(SELECT a.attname::text FROM pg_attribute a
                                WHERE a.attrelid = v_table.tbl_rel AND a.attnum > 0 AND NOT a.attisdropped)),
      'mappable', v_mappable));
  END LOOP;
  v_sql := $q$SELECT l.id, l.at, l.op::text, l.schema_name, l.table_name, l.row_key::text, l.changed,
       l.subject, l.mechanism, l.on_behalf_of, l.trace_id, l.elevated, l.origin
FROM audit_log l
LEFT JOIN jsonb_to_recordset($1) t(relschema text, relname text, keys text[], expected jsonb,
          cols jsonb, allcols text[], columns text[], mappable boolean)
       ON t.relschema = l.schema_name AND t.relname = l.table_name
WHERE l.at >= $2 AND ($3::timestamptz IS NULL OR l.at < $3)
  AND (   (l.scopes = '{}'::jsonb AND $16)
       OR (t.keys IS NOT NULL
           AND ARRAY(SELECT k.k FROM jsonb_object_keys(l.scopes) AS k(k) ORDER BY k.k COLLATE "C") = t.keys
           AND NOT EXISTS (SELECT 1 FROM jsonb_each(t.expected) AS e(k, v) WHERE l.scopes -> e.k IS DISTINCT FROM e.v)
           AND (l.op <> 'U' OR (
                  t.mappable
              AND NOT EXISTS (SELECT 1 FROM jsonb_object_keys(l.changed) AS k(k) WHERE k.k <> ALL (t.columns))
              AND NOT EXISTS (SELECT 1 FROM unnest(t.allcols) AS c(c) WHERE l.changed -> c.c ? 'redacted')
              AND NOT EXISTS (SELECT 1 FROM jsonb_each_text(t.cols) AS sc(scope, col)
                              WHERE l.changed -> sc.col ? 'old'
                                AND l.changed -> sc.col -> 'old' IS DISTINCT FROM t.expected -> sc.scope)))))
  AND ($5 OR l.subject = ANY ($6))
  AND ($7::text IS NULL OR l.subject = $7) AND ($8::text IS NULL OR l.table_name = $8)
  AND ($9::text[] IS NULL OR l.table_name = ANY ($9)) AND ($10::jsonb IS NULL OR l.row_key = $10)
  AND ($11::text IS NULL OR l.trace_id = $11) AND ($12::text IS NULL OR l.op = $12)$q$
    || CASE WHEN v_prefilter IS NULL THEN '' ELSE $q$
  AND l.scopes @> $4$q$ END
    || CASE WHEN p_ascending THEN $q$
  AND ($13::timestamptz IS NULL OR (l.at, l.id) > ($13, $14))
ORDER BY l.at, l.id
LIMIT $15$q$ ELSE $q$
  AND ($13::timestamptz IS NULL OR (l.at, l.id) < ($13, $14))
ORDER BY l.at DESC, l.id DESC
LIMIT $15$q$ END;
  RETURN QUERY EXECUTE v_sql USING v_specs, v_from, v_to, v_prefilter, p_all, p_subjects, p_subject, p_table,
    p_tables, p_row_key, p_trace_id, p_op, p_after_at, p_after_id, p_limit, v_unscoped;
END
$src$, guard_schema()) $$;
CREATE FUNCTION unbounded_tables(c config) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(t.rel::text, ', ' ORDER BY t.rel::text) FROM scoped_table t
  WHERE duplicate_scopes(t.scopes) OR tree_mismatch(t.rel, t.scopes) IS NOT NULL OR NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(t.scopes) x
    JOIN pg_attribute a ON a.attrelid = t.rel AND a.attname = x->>'col' AND a.attnum > 0 AND NOT a.attisdropped
    WHERE normal_scope(x) @> '{"on": "both", "elevable": false}'::jsonb
      AND scope_of(x) = c.boundary_scope AND format_type(a.atttypid, NULL) = c.boundary_type) $$;
CREATE FUNCTION apply_audit_reading() RETURNS void LANGUAGE plpgsql AS $$
DECLARE c config; bad text; reader text;
BEGIN
  SELECT * INTO c FROM config;
  IF NOT coalesce(is_member(session_user, c.owner_role), false) THEN
    PERFORM refuse('only the owner may apply audit reading'); END IF;
  IF audit_log_rel() IS NULL THEN RETURN; END IF;
  IF c.audit_reading AND c.boundary_scope IS NOT NULL THEN
    bad := unbounded_tables(c);
    IF bad IS NOT NULL THEN
      PERFORM reject(format('audit reading needs boundary %s (%s) on every scoped table; it differs on: %s',
                            c.boundary_scope, c.boundary_type, bad)); END IF;
  END IF;
  PERFORM open_hatch();
  reader := format('%I.read_audit_log(%s)', c.app_schema,
    'p_all boolean, p_subjects text[], p_cap integer, p_from timestamp with time zone, p_to timestamp with time zone, p_subject text, p_table text, p_tables text[], p_row_key jsonb, p_trace_id text, p_op text, p_after_at timestamp with time zone, p_after_id bigint, p_ascending boolean, p_limit integer');
  EXECUTE format('CREATE OR REPLACE FUNCTION %s RETURNS TABLE (id bigint, at timestamptz, op text, schema_name text, '
                 'table_name text, row_key text, changed jsonb, subject text, mechanism text, on_behalf_of text, '
                 'trace_id text, elevated text[], origin text) LANGUAGE plpgsql STABLE SECURITY DEFINER '
                 'SET search_path = pg_catalog, %I, pg_temp AS %L', reader, c.app_schema, audit_reader_source());
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', reader);
  IF c.audit_reading THEN
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', reader, c.readers_role);
  ELSE
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM %I', reader, c.readers_role);
  END IF;
END $$;
CREATE FUNCTION audit_log_readers() RETURNS SETOF text LANGUAGE sql STABLE AS $$
  WITH RECURSIVE reads(rel) AS (
    SELECT t.relid::oid FROM pg_partition_tree(audit_log_rel()) t
    UNION
    SELECT rw.ev_class FROM reads
      JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.refclassid = 'pg_class'::regclass AND d.refobjid = reads.rel
      JOIN pg_rewrite rw ON rw.oid = d.objid
      JOIN pg_class v ON v.oid = rw.ev_class AND v.relkind IN ('v', 'm')
    WHERE rw.ev_class <> reads.rel)
  SELECT format('%I.%I', n.nspname, c.relname) FROM reads JOIN pg_class c ON c.oid = reads.rel JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('v', 'm') AND NOT EXISTS (
    SELECT 1 FROM pg_options_to_table(c.reloptions) o WHERE o.option_name = 'security_invoker' AND o.option_value::boolean)
  UNION
  SELECT format('%I.%I rule %I', n.nspname, c.relname, rw.rulename)
  FROM pg_rewrite rw JOIN pg_class c ON c.oid = rw.ev_class JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE rw.rulename <> '_RETURN' AND EXISTS (
    SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_rewrite'::regclass AND d.objid = rw.oid
      AND d.refclassid = 'pg_class'::regclass AND d.refobjid IN (SELECT rel FROM reads)) $$;
CREATE FUNCTION refuse_audit_log_views() RETURNS void LANGUAGE plpgsql AS $$
DECLARE bad text;
BEGIN
  IF app_schema() IS NULL THEN RETURN; END IF;
  IF audit_log_rel() IS NULL THEN RETURN; END IF;
  SELECT string_agg(r, ', ' ORDER BY r) INTO bad FROM audit_log_readers() r;
  IF bad IS NOT NULL THEN
    PERFORM violation(format('views or rules read the audit log with their owner''s rights, past every boundary: %s; '
                             'drop them (or recreate a view with security_invoker), then rerun the bootstrap', bad)); END IF;
END $$;
SELECT refuse_audit_log_views();
CREATE OR REPLACE FUNCTION assert_audited_schema() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  bad text; cfg config; log regclass := audit_log_rel(); writer regprocedure; refuser regprocedure;
  record_trigger name := 'loom_audit_record';
  row_trigger name := 'loom_audit_log_immutable_row';
  truncate_trigger name := 'loom_audit_log_immutable_truncate';
  readers bigint; reader oid; tree oid[];
BEGIN
  SELECT * INTO cfg FROM config;
  writer := to_regprocedure(format('%I.write_audit_row()', cfg.app_schema));
  refuser := to_regprocedure(quote_ident(guard_schema()) || '.refuse_audit_log_change()');
  SELECT count(*), min(p.oid) INTO readers, reader FROM pg_proc p
    WHERE p.pronamespace = to_regnamespace(quote_ident(cfg.app_schema)) AND p.proname = 'read_audit_log';
  IF readers > 1 THEN PERFORM violation(format('%s.read_audit_log is overloaded', cfg.app_schema)); END IF;
  IF readers = 1 THEN
    IF NOT EXISTS (
         SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner JOIN pg_language g ON g.oid = p.prolang
         WHERE p.oid = reader AND r.rolname = cfg.owner_role AND g.lanname = 'plpgsql' AND p.prosecdef AND p.provolatile = 's'
           AND p.proconfig = ARRAY[format('search_path=pg_catalog, %I, pg_temp', cfg.app_schema)]
           AND pg_get_function_identity_arguments(p.oid) = 'p_all boolean, p_subjects text[], p_cap integer, p_from timestamp with time zone, p_to timestamp with time zone, p_subject text, p_table text, p_tables text[], p_row_key jsonb, p_trace_id text, p_op text, p_after_at timestamp with time zone, p_after_id bigint, p_ascending boolean, p_limit integer'
           AND pg_get_function_result(p.oid) = 'TABLE(id bigint, at timestamp with time zone, op text, schema_name text, table_name text, row_key text, changed jsonb, subject text, mechanism text, on_behalf_of text, trace_id text, elevated text[], origin text)'
           AND p.prosrc = audit_reader_source()) THEN
      PERFORM violation(format('the audit reader %s.read_audit_log differs from the released one', cfg.app_schema)); END IF;
    SELECT string_agg(coalesce(g.rolname, 'PUBLIC (every role)') || ':' || a.privilege_type, ', ') INTO bad
      FROM pg_proc p, LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a LEFT JOIN pg_roles g ON g.oid = a.grantee
      WHERE p.oid = reader AND a.grantee <> p.proowner
        AND NOT coalesce(cfg.audit_reading AND g.rolname = cfg.readers_role AND a.privilege_type = 'EXECUTE' AND NOT a.is_grantable, false);
    IF bad IS NOT NULL THEN PERFORM violation(format('audit reader grant outside {readers: EXECUTE while reading}: %s', bad)); END IF;
    IF cfg.boundary_scope IS NOT NULL AND EXISTS (
         SELECT 1 FROM pg_proc p, LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
         WHERE p.oid = reader AND a.grantee <> p.proowner) THEN
      bad := unbounded_tables(cfg);
      IF bad IS NOT NULL THEN
        PERFORM violation(format('the audit reader is granted and these tables lack boundary %s (%s): %s',
                                 cfg.boundary_scope, cfg.boundary_type, bad)); END IF;
    END IF;
  END IF;
  IF log IS NULL THEN
    IF EXISTS (SELECT 1 FROM audited_table) THEN PERFORM violation('audited tables without an audit log'); END IF;
    -- Registered partitions mean a log was made: it was moved or renamed, not dropped (that is refused).
    IF EXISTS (SELECT 1 FROM audit_partition) THEN
      PERFORM violation(format('the audit log has registered partitions and is not %I.audit_log', cfg.app_schema)); END IF;
    RETURN;
  END IF;
  -- The log and its direct partitions: the checks below make that the whole tree (only plain tables are registered).
  tree := ARRAY(SELECT log::oid UNION ALL SELECT inhrelid FROM pg_inherits WHERE inhparent = log);
  PERFORM refuse_audit_log_views();
  IF writer IS NULL OR NOT EXISTS (
       SELECT 1 FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner
       WHERE p.oid = writer AND p.prosecdef AND r.rolname = cfg.owner_role AND p.prosrc = audit_trigger_source()
         AND p.proconfig = ARRAY[format('search_path=pg_catalog, %I, pg_temp', cfg.app_schema)]) THEN
    PERFORM violation(format('the audit writer %s.write_audit_row differs from the released one', cfg.app_schema)); END IF;
  SELECT string_agg(a.rel::text, ', ') INTO bad FROM audited_table a WHERE NOT EXISTS (
    SELECT 1 FROM pg_trigger g WHERE g.tgrelid = a.rel AND g.tgname = record_trigger AND g.tgenabled = 'A' AND g.tgfoid = writer
      AND (g.tgtype & 1) <> 0 AND (g.tgtype & 2) = 0 AND (g.tgtype & 28) = 28 AND g.tgqual IS NULL AND g.tgattr = ''::int2vector);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit trigger missing, disabled, wrong function or wrong events: %s', bad)); END IF;
  SELECT string_agg(a.rel::text, ', ') INTO bad FROM audited_table a WHERE NOT EXISTS (
    SELECT 1 FROM pg_index i WHERE i.indrelid = a.rel AND i.indisprimary);
  IF bad IS NOT NULL THEN PERFORM violation(format('audited tables without a primary key: %s', bad)); END IF;
  SELECT string_agg(a.rel::text, ', ') INTO bad FROM audited_table a WHERE EXISTS (
    SELECT 1 FROM pg_index i JOIN pg_attribute t ON t.attrelid = i.indrelid AND t.attnum = ANY (i.indkey)
    WHERE i.indrelid = a.rel AND i.indisprimary AND t.attname::text = ANY (a.excluded));
  IF bad IS NOT NULL THEN PERFORM violation(format('audited tables keyed by an excluded column: %s', bad)); END IF;
  SELECT string_agg(t.relid::text, ', ') INTO bad FROM audited_table a, pg_partition_tree(a.rel) t
    WHERE t.relid <> a.rel::oid AND NOT EXISTS (
      SELECT 1 FROM pg_trigger g WHERE g.tgrelid = t.relid AND g.tgname = record_trigger AND g.tgenabled IN ('A', 'O') AND g.tgfoid = writer);
  IF bad IS NOT NULL THEN PERFORM violation(format('partitions of audited tables without the audit trigger: %s', bad)); END IF;
  SELECT string_agg(x.r::regclass::text, ', ') INTO bad FROM unnest(tree) AS x(r)
    WHERE NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.r AND g.tgname = row_trigger AND g.tgenabled = 'A'
                        AND g.tgfoid = refuser AND (g.tgtype & 1) <> 0 AND (g.tgtype & 2) <> 0 AND (g.tgtype & 24) = 24)
       OR NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = x.r AND g.tgname = truncate_trigger AND g.tgenabled = 'A'
                        AND g.tgfoid = refuser AND (g.tgtype & 1) = 0 AND (g.tgtype & 32) <> 0);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log immutability triggers missing or disabled: %s', bad)); END IF;
  SELECT string_agg(x.relid::regclass::text || '.' || quote_ident(g.tgname), ', ') INTO bad
    FROM pg_partition_tree(log) x JOIN pg_trigger g ON g.tgrelid = x.relid
    WHERE NOT g.tgisinternal AND NOT (g.tgname IN (row_trigger, truncate_trigger) AND g.tgfoid = refuser);
  IF bad IS NOT NULL THEN
    PERFORM violation(format('triggers on the audit log run on every audited write with their owner''s rights: %s; '
                             'drop them, then rerun the bootstrap', bad)); END IF;
  SELECT string_agg(DISTINCT c.relname || ':' || a.privilege_type || '->' || coalesce(g.rolname, 'PUBLIC'), ', ') INTO bad
    FROM unnest(tree) AS x(r)
    JOIN pg_class c ON c.oid = x.r, LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
    WHERE a.grantee <> c.relowner AND NOT (g.rolname IS NOT DISTINCT FROM cfg.auditor_role AND a.privilege_type = 'SELECT');
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log grant outside {auditor: SELECT}: %s', bad)); END IF;
  SELECT string_agg(DISTINCT x.r::regclass::text || '.' || quote_ident(a.attname), ', ') INTO bad
    FROM unnest(tree) AS x(r) JOIN pg_attribute a ON a.attrelid = x.r WHERE a.attacl IS NOT NULL;
  IF bad IS NOT NULL THEN PERFORM violation(format('column grants on the audit log are not allowed: %s', bad)); END IF;
  SELECT string_agg(i.inhrelid::regclass::text, ', ') INTO bad FROM pg_inherits i
    WHERE i.inhparent = log AND i.inhrelid NOT IN (SELECT rel::oid FROM audit_partition);
  IF bad IS NOT NULL THEN PERFORM violation(format('unregistered audit log partitions: %s', bad)); END IF;
  SELECT string_agg(p.rel::text, ', ') INTO bad FROM audit_partition p
    WHERE NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = p.rel AND i.inhparent = log);
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log partitions detached: %s', bad)); END IF;
  SELECT string_agg(p.rel::text, ', ') INTO bad FROM audit_partition p JOIN pg_class c ON c.oid = p.rel WHERE c.relkind <> 'r';
  IF bad IS NOT NULL THEN PERFORM violation(format('audit log partitions that are not plain tables: %s', bad)); END IF;
END $$;
-- How a guard costs DDL (T12c). Every store's guard has its own event triggers, so every DDL in
-- the database wakes every guard. A guard asserts only DDL that can change what it asserts, and
-- never at the statement: it enters its hatch, whose deferred close_hatch runs the full assertion
-- once, at COMMIT. Refused at the statement still: DDL on the guard schema, and dropping a
-- registered table, the audit log or one of its registered partitions.
--
-- Every DDL wakes every guard, so the path that finds nothing to do must stay cheap however many
-- stores share the database. Each guard function pins search_path, and that costs more per call
-- than a catalogue lookup: the relevance checks below read the guard's names once, call one small
-- function per command (may_expose) and none per catalogue row, and look the guard's rows up by
-- oid, never by scanning a catalogue.
--
-- Relations and every table they sit under, at any depth, walked through pg_inherits:
-- pg_partition_tree would lock every partition, on every DDL, in every guard. PL/pgSQL keeps the
-- plan; a SQL function is planned again on every call.
CREATE FUNCTION lineage(rels oid[]) RETURNS SETOF oid LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN QUERY
    WITH RECURSIVE up(rel) AS (SELECT unnest(rels) UNION SELECT i.inhparent FROM pg_inherits i JOIN up ON i.inhrelid = up.rel)
    SELECT up.rel FROM up WHERE up.rel IS NOT NULL;
END $$;
-- The relations a guard asserts wherever they live: its registered and audited tables, its audit
-- log tree, and every table above any of them (a parent reads the rows of its children). The log is
-- anchored by oid through its registered partitions too, so moving or renaming it still counts.
-- UNION ALL, not UNION: the guard tables are never analysed, and hashing their estimated rows
-- costs ten times the walk. Inheritance has no cycles, and a repeated oid is harmless.
-- The anchor through the parents of partitions is defense in depth: lineage(audit_partition) reaches the log too.
CREATE FUNCTION guarded_relations() RETURNS SETOF oid LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN QUERY
    WITH RECURSIVE tree(rel) AS (SELECT audit_log_rel()::oid
                                 UNION ALL SELECT i.inhparent FROM audit_partition p JOIN pg_inherits i ON i.inhrelid = p.rel
                                 UNION ALL SELECT i.inhrelid FROM pg_inherits i JOIN tree t ON i.inhparent = t.rel)
    SELECT lineage(ARRAY(SELECT s.rel::oid FROM scoped_table s UNION ALL SELECT a.rel::oid FROM audited_table a
                         UNION ALL SELECT p.rel::oid FROM audit_partition p UNION ALL SELECT t.rel FROM tree t));
END $$;
CREATE FUNCTION reader_name() RETURNS name LANGUAGE sql IMMUTABLE AS $$ SELECT 'read_audit_log'::name $$;
-- GRANT and REVOKE name neither their object nor its schema. Each rewrites the catalogue row it
-- changes, so a guard checks whether this transaction rewrote (by xmin) one of the rows its
-- assertion reads privileges from: its relations (rels, from guarded_relations) and their columns
-- (a column grant writes only pg_attribute), the reader, its schemas. A transaction that began
-- after this one and has committed counts too: that only asserts more.
CREATE FUNCTION acl_written(app name, grd name, rels oid[]) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
  now bigint := current_xid() % 4294967296;
BEGIN
  RETURN EXISTS (
    SELECT 1 FROM (SELECT k.xmin FROM pg_class k WHERE k.oid = ANY (rels)
                   UNION ALL SELECT a.xmin FROM pg_attribute a WHERE a.attrelid = ANY (rels)
                   UNION ALL SELECT p.xmin FROM pg_proc p
                             WHERE p.proname = reader_name() AND p.pronamespace = to_regnamespace(quote_ident(app))
                   UNION ALL SELECT n.xmin FROM pg_namespace n WHERE n.nspname IN (app, grd)) w(x)
    WHERE (w.x::text::bigint - now + 4294967296) % 4294967296 < 2147483648);
END $$;
-- What a command on a schema other than the guard's may do without waking it.
CREATE FUNCTION namespace_tags() RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['CREATE SCHEMA', 'ALTER SCHEMA', 'COMMENT', 'SECURITY LABEL'] $$;
-- The commands that name no schema and cannot change what any guard asserts. Any other command
-- naming no schema wakes every guard (fail closed), so an unknown or new tag is always asserted.
CREATE FUNCTION harmless_tags() RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY[
    'ALTER DEFAULT PRIVILEGES',                                  -- later objects; their own DDL is asserted
    'CREATE PUBLICATION', 'ALTER PUBLICATION',                   -- row copies: a later revision (T12b)
    'CREATE SUBSCRIPTION', 'ALTER SUBSCRIPTION',
    'CREATE LANGUAGE', 'CREATE CAST', 'CREATE ACCESS METHOD',    -- no relation, policy or privilege
    'CREATE FOREIGN DATA WRAPPER', 'ALTER FOREIGN DATA WRAPPER',
    'CREATE SERVER', 'ALTER SERVER', 'CREATE USER MAPPING', 'ALTER USER MAPPING'] $$;
-- The dropped objects naming no schema that cannot change what any guard asserts, as above.
CREATE FUNCTION harmless_drops() RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['default acl', 'publication', 'publication relation', 'publication namespace', 'subscription',
               'language', 'cast', 'access method', 'foreign-data wrapper', 'server', 'user mapping'] $$;
-- A command that may make a view or a rule read the audit log: the walk must run again. A view or
-- a rule anywhere can read the log, so such a command wakes every guard that has one. Every
-- command on a rule reports it as one; a refresh changes neither a definition nor an option.
CREATE FUNCTION may_expose(object_type text, tag text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT object_type IN ('view', 'materialized view', 'rule') AND tag <> 'REFRESH MATERIALIZED VIEW' $$;
-- Whether a command of the ending DDL can change what this guard asserts. A relation counts if it,
-- or any table above it, is in the store or guard schema or is one of the relations of the guard
-- (guarded_relations holds every table above those too). Triggers, policies and rules report no
-- schema: they count through the relation they sit on, and fail closed when it cannot be found.
-- Both schemas are found by name: once one no longer resolves (renamed, dropped), every DDL counts.
-- A view or rule on a temporary relation is private to its session (any role has TEMP): it counts
-- only when its rules read a relation of the guard or any view (a reader of the log may be one).
CREATE FUNCTION ddl_wakes_guard(app name, grd name) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
DECLARE
  spaces oid[] := ARRAY[to_regnamespace(quote_ident(app))::oid, to_regnamespace(quote_ident(grd))::oid];
  guarded oid[];
  has_log boolean;
BEGIN
  IF array_position(spaces, NULL) IS NOT NULL THEN RETURN true; END IF;
  guarded := ARRAY(SELECT guarded_relations());
  has_log := audit_log_of(app) IS NOT NULL OR EXISTS (SELECT 1 FROM audit_partition);
  RETURN EXISTS (
    SELECT 1 FROM pg_event_trigger_ddl_commands() c,
      -- A catalogue row's tableoid is its catalogue, so the object is looked up only where it lives.
      LATERAL (SELECT coalesce((SELECT k.oid FROM pg_class k WHERE k.oid = c.objid AND k.tableoid = c.classid),
                               (SELECT g.tgrelid FROM pg_trigger g WHERE g.oid = c.objid AND g.tableoid = c.classid),
                               (SELECT p.polrelid FROM pg_policy p WHERE p.oid = c.objid AND p.tableoid = c.classid),
                               (SELECT w.ev_class FROM pg_rewrite w WHERE w.oid = c.objid AND w.tableoid = c.classid)),
                      c.object_type IN ('trigger', 'policy', 'rule')) r(rel, on_relation)
    WHERE c.schema_name IN (app, grd)
       OR (c.classid = 'pg_namespace'::regclass AND (c.objid = ANY (spaces) OR c.command_tag <> ALL (namespace_tags())))
       OR (r.on_relation AND r.rel IS NULL)
       OR (r.rel IS NOT NULL AND EXISTS (SELECT 1 FROM lineage(ARRAY[r.rel]) p(oid) JOIN pg_class k ON k.oid = p.oid
                                         WHERE k.relnamespace = ANY (spaces) OR p.oid = ANY (guarded)))
       OR (c.command_tag IN ('GRANT', 'REVOKE') AND acl_written(app, grd, guarded))
       OR (c.schema_name IS NULL AND c.command_tag NOT IN ('GRANT', 'REVOKE')
           AND c.classid IS DISTINCT FROM 'pg_namespace'::regclass AND NOT r.on_relation
           AND c.command_tag <> ALL (harmless_tags()))
       OR (has_log AND may_expose(c.object_type, c.command_tag)
           AND (NOT EXISTS (SELECT 1 FROM pg_class k WHERE k.oid = r.rel AND k.relpersistence = 't')
                OR EXISTS (SELECT 1 FROM pg_rewrite w
                             JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = w.oid
                                             AND d.refclassid = 'pg_class'::regclass AND d.refobjid <> w.ev_class
                             JOIN pg_class v ON v.oid = d.refobjid
                           WHERE w.ev_class = r.rel AND (v.oid = ANY (guarded) OR v.relkind IN ('v', 'm'))))));
END $$;
-- Whether an object the ending DDL dropped can change what this guard asserts. Before Postgres 15
-- a dropped trigger or policy carries no schema: its address names its schema and table. As above,
-- once either schema no longer resolves by name, every drop counts.
-- That schema check is defense in depth: on_ddl_end also runs for a DROP, and ddl_wakes_guard has it too.
CREATE FUNCTION drop_wakes_guard(app name, grd name) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF to_regnamespace(quote_ident(app)) IS NULL OR to_regnamespace(quote_ident(grd)) IS NULL THEN RETURN true; END IF;
  RETURN EXISTS (
    SELECT 1 FROM pg_event_trigger_dropped_objects() d,
      LATERAL (SELECT d.object_type IN ('trigger', 'policy', 'rule')) r(on_relation)
    WHERE d.schema_name IN (app, grd)
       OR (d.object_type = 'schema' AND d.object_name IN (app, grd))
       OR d.objid IN (SELECT guarded_relations())
       OR (r.on_relation
           AND (coalesce(cardinality(d.address_names), 0) < 2 OR d.address_names[1] IN (app, grd)
                OR to_regclass(format('%I.%I', d.address_names[1], d.address_names[2])) IN (SELECT guarded_relations())))
       OR (d.schema_name IS NULL AND d.object_type <> 'schema' AND NOT r.on_relation
           AND d.object_type <> ALL (harmless_drops())));
END $$;
CREATE OR REPLACE FUNCTION on_ddl_end() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE app name := app_schema(); grd name := guard_schema();
BEGIN
  PERFORM forbid_guard_ddl(EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() c WHERE c.schema_name = grd));
  IF app IS NULL OR hatch_open() THEN RETURN; END IF;
  IF ddl_wakes_guard(app, grd) THEN PERFORM enter_hatch(); END IF;
END $$;
CREATE OR REPLACE FUNCTION on_sql_drop() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE bad text; wakes boolean := false; app name := app_schema(); grd name := guard_schema();
BEGIN
  PERFORM forbid_guard_ddl(EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() d
                                   WHERE d.schema_name = grd OR d.object_identity = quote_ident(grd)));
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table') AND d.objid IN (SELECT rel::oid FROM scoped_table);
  IF bad IS NOT NULL THEN PERFORM violation(format('unprotect before dropping a registered table: %s', bad)); END IF;
  SELECT string_agg(object_identity, ', ') INTO bad FROM pg_event_trigger_dropped_objects() d
    WHERE d.object_type IN ('table','partitioned table')
      AND ((d.schema_name = app AND d.object_name = 'audit_log') OR d.objid IN (SELECT rel::oid FROM audit_partition));
  IF bad IS NOT NULL THEN PERFORM violation(format('the audit log is dropped only by drop_expired_audit_partition: %s', bad)); END IF;
  IF app IS NOT NULL AND NOT hatch_open() THEN wakes := drop_wakes_guard(app, grd); END IF;
  DELETE FROM audited_table a WHERE NOT EXISTS (SELECT 1 FROM pg_class k WHERE k.oid = a.rel);
  IF wakes THEN PERFORM enter_hatch(); END IF;
END $$;
CREATE OR REPLACE FUNCTION configure(doc jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  wanted config := jsonb_populate_record(NULL::config, doc);
  cfg config;
  configured boolean;
  spec record;
  holder name;
  bad text;
  fn regprocedure;
  stripped constant text[] := ARRAY['users', 'auditor_role', 'retention_months', 'audit_reading', 'boundary_scope', 'boundary_type'];
BEGIN
  wanted.singleton := true;
  wanted.installer := current_user;
  wanted.audit_reading := coalesce((doc->>'audit_reading')::boolean, false);
  SELECT * INTO cfg FROM config;
  configured := FOUND;
  IF configured AND to_jsonb(cfg) - stripped IS DISTINCT FROM to_jsonb(wanted) - stripped THEN
    PERFORM refuse(format('guard %s is configured for another schema, other roles, other version tables or another installer', guard_schema())); END IF;
  IF configured AND cfg.auditor_role IS NOT NULL AND wanted.auditor_role IS DISTINCT FROM cfg.auditor_role THEN
    PERFORM refuse(format('guard %s already has the auditor role %s; it never changes', guard_schema(), cfg.auditor_role)); END IF;
  IF coalesce((doc->>'clear_audit_boundary')::boolean, false) THEN
    IF wanted.audit_reading THEN PERFORM refuse('disable audit reading before clearing its boundary'); END IF;
    wanted.boundary_scope := NULL; wanted.boundary_type := NULL;
  ELSIF configured AND cfg.boundary_scope IS NOT NULL THEN
    IF wanted.boundary_scope IS NOT NULL
       AND (wanted.boundary_scope, format_type(to_regtype(wanted.boundary_type), NULL))
           IS DISTINCT FROM (cfg.boundary_scope, cfg.boundary_type) THEN
      PERFORM refuse(format('guard %s reads the audit log within boundary %s (%s); it changes only by '
                            'disabling reading and clearing it', guard_schema(), cfg.boundary_scope, cfg.boundary_type)); END IF;
    wanted.boundary_scope := cfg.boundary_scope; wanted.boundary_type := cfg.boundary_type;
  ELSIF wanted.audit_reading AND wanted.boundary_scope IS NOT NULL THEN
    wanted.boundary_type := format_type(to_regtype(wanted.boundary_type), NULL);
    IF wanted.boundary_type IS NULL THEN PERFORM reject('the boundary column type is not a known type'); END IF;
  ELSE
    wanted.boundary_scope := NULL; wanted.boundary_type := NULL;
  END IF;
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
  IF NOT wanted.audit_reading THEN
    FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p
              WHERE p.pronamespace = to_regnamespace(quote_ident(wanted.app_schema)) AND p.proname = 'read_audit_log' LOOP
      EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM %I', fn, wanted.readers_role);
      IF EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE p.oid = fn AND a.grantee = to_regrole(quote_ident(wanted.readers_role))) THEN
        PERFORM refuse(format('could not revoke %s from %s; run the bootstrap as a role that can', fn, wanted.readers_role)); END IF;
    END LOOP;
  END IF;
  INSERT INTO config SELECT (wanted).* ON CONFLICT (singleton) DO UPDATE
    SET users = EXCLUDED.users, auditor_role = EXCLUDED.auditor_role, retention_months = EXCLUDED.retention_months,
        audit_reading = EXCLUDED.audit_reading, boundary_scope = EXCLUDED.boundary_scope, boundary_type = EXCLUDED.boundary_type;
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
