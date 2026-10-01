CREATE FUNCTION pg_temp.loom_create_guard(guard name, min_version int) RETURNS void LANGUAGE plpgsql AS $$
DECLARE guard_owner name;
BEGIN
  IF current_setting('server_version_num')::int < min_version THEN
    RAISE EXCEPTION 'loom_guard[%]: server_version_num % is below %', guard, current_setting('server_version_num'), min_version USING ERRCODE = '22023';
  END IF;
  IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname = 'rds_superuser' AND pg_catalog.pg_has_role(current_user, r.oid, 'MEMBER')) THEN
    RAISE EXCEPTION 'loom_guard[%]: creating an event trigger needs superuser or rds_superuser; current_user is %', guard, current_user USING ERRCODE = '42501';
  END IF;
  SELECT r.rolname INTO guard_owner FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner WHERE n.nspname = guard;
  IF FOUND THEN
    IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = guard_owner) AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles r WHERE r.rolname = 'rds_superuser' AND pg_catalog.pg_has_role(guard_owner, r.oid, 'MEMBER')) THEN
      RAISE EXCEPTION 'loom_guard[%]: schema % exists and is owned by %, not by a superuser; the bootstrap never adopts it', guard, guard, guard_owner USING ERRCODE = '42501';
    END IF;
    IF pg_catalog.to_regclass(pg_catalog.format('%I.revision', guard)) IS NULL THEN
      RAISE EXCEPTION 'loom_guard[%]: schema % exists without loom revisions; the bootstrap never adopts it', guard, guard USING ERRCODE = '42501';
    END IF;
  ELSE
    EXECUTE pg_catalog.format('CREATE SCHEMA %I', guard);
    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', guard);
    EXECUTE pg_catalog.format('CREATE TABLE %I.revision (n int PRIMARY KEY, sha256 text NOT NULL)', guard);
  END IF;
END $$;
