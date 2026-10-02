CREATE OR REPLACE FUNCTION owner_functions() RETURNS name[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['protect_scoped_table', 'unprotect_scoped_table', 'open_hatch', 'assert_scoped_schema',
               'grant_table', 'grant_privileges', 'check_privileges', 'prepare_version_tables', 'limit_version_table',
               'registered_tables', 'guard_schema', 'app_schema', 'owner_role', 'group_role', 'users_of',
               'tagged', 'reject', 'refuse', 'is_member', 'commands', 'authorize',
               'create_range_partition', 'detach_partition']::name[] $$;
CREATE FUNCTION create_range_partition(parent regclass, partition_name text, lower_bound text, upper_bound text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE registered scoped_table; qualified text := format('%I.%I', app_schema(), partition_name); part regclass;
BEGIN
  PERFORM authorize(parent, 'partition');
  IF coalesce(octet_length(partition_name), 0) NOT BETWEEN 1 AND 63 THEN
    PERFORM reject(format('partition name %s is empty or longer than 63 bytes', partition_name)); END IF;
  SELECT * INTO registered FROM scoped_table WHERE rel = parent;
  IF NOT FOUND THEN PERFORM refuse(format('%s is not a registered scoped table', parent)); END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = parent AND partstrat = 'r') THEN
    PERFORM reject(format('%s is not partitioned by range', parent)); END IF;
  part := to_regclass(qualified);
  IF part IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = part AND inhparent = parent)
       AND EXISTS (SELECT 1 FROM scoped_table WHERE rel = part) THEN RETURN false; END IF;
    PERFORM refuse(format('%s exists and is not a protected partition of %s', part, parent));
  END IF;
  PERFORM open_hatch();
  EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)', qualified, parent, lower_bound, upper_bound);
  PERFORM protect_scoped_table(to_regclass(qualified), registered.scopes, registered.privileges);
  RETURN true;
END $$;
CREATE FUNCTION detach_partition(parent regclass, partition_name text, drop_table boolean) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE part regclass;
BEGIN
  PERFORM authorize(parent, 'detach partitions of');
  IF coalesce(octet_length(partition_name), 0) NOT BETWEEN 1 AND 63 THEN
    PERFORM reject(format('partition name %s is empty or longer than 63 bytes', partition_name)); END IF;
  part := to_regclass(format('%I.%I', app_schema(), partition_name));
  IF part IS NULL THEN RETURN false; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = part AND inhparent = parent) THEN
    IF drop_table THEN
      PERFORM refuse(format('%s is not a partition of %s; a detached table is dropped by hand', part, parent)); END IF;
    RETURN false;
  END IF;
  PERFORM open_hatch();
  PERFORM unprotect_scoped_table(part);
  EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', parent, part);
  IF drop_table THEN EXECUTE format('DROP TABLE %s', part); END IF;
  RETURN true;
END $$;
DO $$
DECLARE fn regprocedure;
BEGIN
  FOR fn IN SELECT p.oid::regprocedure FROM pg_proc p WHERE p.pronamespace = current_schema()::regnamespace AND p.proconfig IS NULL LOOP
    EXECUTE format('ALTER FUNCTION %s SET search_path = pg_catalog, %I, pg_temp', fn, current_schema());
  END LOOP;
  EXECUTE format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', current_schema());
END $$;
