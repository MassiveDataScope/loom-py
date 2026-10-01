CREATE FUNCTION pg_temp.loom_create_guard(guard name, min_version int) RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $$
DECLARE guard_owner name; privileged name[];
BEGIN
  IF current_setting('server_version_num')::int < min_version THEN
    RAISE invalid_parameter_value USING MESSAGE = format('loom_guard[%s]: server_version_num %s is below %s', guard, current_setting('server_version_num'), min_version);
  END IF;
  privileged := ARRAY(SELECT r.rolname FROM pg_roles r WHERE r.rolsuper
                      OR EXISTS (SELECT 1 FROM pg_roles a WHERE a.rolname = 'rds_superuser' AND pg_has_role(r.oid, a.oid, 'MEMBER')));
  IF NOT current_user = ANY (privileged) THEN
    RAISE insufficient_privilege USING MESSAGE = format('loom_guard[%s]: creating an event trigger needs superuser or rds_superuser; current_user is %s', guard, current_user);
  END IF;
  SELECT r.rolname INTO guard_owner FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = guard;
  IF NOT FOUND THEN
    EXECUTE format('CREATE SCHEMA %I', guard);
    EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', guard);
    EXECUTE format('CREATE TABLE %I.revision (n int PRIMARY KEY, sha256 text NOT NULL)', guard);
  ELSIF guard_owner IS DISTINCT FROM current_user THEN
    RAISE insufficient_privilege USING MESSAGE = format('loom_guard[%s]: schema %s exists and is owned by %s, not by the installing role %s; the bootstrap never adopts it', guard, guard, guard_owner, current_user);
  ELSIF to_regclass(format('%I.revision', guard)) IS NULL THEN
    RAISE insufficient_privilege USING MESSAGE = format('loom_guard[%s]: schema %s exists without loom revisions; the bootstrap never adopts it', guard, guard);
  END IF;
END $$;
