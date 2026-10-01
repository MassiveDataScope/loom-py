-- Product tables of the three synthetic products, created as each owner and protected through the guard.
-- Mirrors gate-evidence/fixture-round7.tail.sql so every scenario label keeps its meaning.
SET ROLE notes_owner;
CREATE TABLE notes.notes (owner_id uuid NOT NULL, id serial, editor varchar(64) NOT NULL, body text NOT NULL,
  PRIMARY KEY (owner_id, id), UNIQUE (owner_id, editor));
CREATE TABLE notes.note_items (owner_id uuid NOT NULL, id serial, note_id integer NOT NULL, name text NOT NULL,
  PRIMARY KEY (owner_id, id), FOREIGN KEY (owner_id, note_id) REFERENCES notes.notes (owner_id, id) ON DELETE CASCADE);
CREATE TABLE notes.note_events (owner_id uuid NOT NULL, at date NOT NULL, kind text NOT NULL) PARTITION BY RANGE (at);
CREATE TABLE notes.note_events_2026 PARTITION OF notes.note_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE notes.note_kinds (id serial PRIMARY KEY, name text NOT NULL);
GRANT SELECT ON notes.note_kinds TO notes_readers;
SELECT loom_guard_notes.protect_scoped_table('notes.notes',
  '[{"col":"owner_id","scope":"owner","on":"both"},{"col":"editor","scope":"editor","on":"write","elevable":true}]',
  ARRAY['SELECT','INSERT','UPDATE','DELETE']);
SELECT loom_guard_notes.protect_scoped_table('notes.note_items', '[{"col":"owner_id","scope":"owner","on":"both"}]',
  ARRAY['SELECT','INSERT','UPDATE','DELETE']);
SELECT loom_guard_notes.protect_scoped_table('notes.note_events', '[{"col":"owner_id","scope":"owner","on":"both"}]',
  ARRAY['SELECT']);
SELECT loom_guard_notes.protect_scoped_table('notes.note_events_2026', '[{"col":"owner_id","scope":"owner","on":"both"}]',
  ARRAY['SELECT']);
RESET ROLE;
SET ROLE sites_owner;
CREATE TABLE sites.site_readings (region integer NOT NULL, id serial, value numeric NOT NULL, PRIMARY KEY (region, id));
CREATE TABLE sites.site_archive (region integer NOT NULL, id serial, payload text, PRIMARY KEY (region, id));
SELECT loom_guard_sites.protect_scoped_table('sites.site_readings', '[{"col":"region","scope":"region","on":"both"}]',
  ARRAY['SELECT','INSERT','UPDATE','DELETE']);
SELECT loom_guard_sites.protect_scoped_table('sites.site_archive', '[{"col":"region","scope":"region","on":"both"}]',
  ARRAY['SELECT']);
RESET ROLE;
SET ROLE ledger_owner;
CREATE TABLE ledger.accounts (id bigint PRIMARY KEY, name text NOT NULL);
CREATE TABLE ledger.entries (account_id bigint NOT NULL REFERENCES ledger.accounts (id) ON DELETE RESTRICT, id serial,
  amount numeric NOT NULL, PRIMARY KEY (account_id, id));
GRANT SELECT ON ledger.accounts TO ledger_readers;
SELECT loom_guard_ledger.protect_scoped_table('ledger.entries', '[{"col":"account_id","scope":"account","on":"both"}]',
  ARRAY['SELECT','INSERT','UPDATE','DELETE']);
RESET ROLE;
INSERT INTO notes.notes (owner_id, editor, body) VALUES
  ('11111111-1111-1111-1111-111111111111','ana','ana-1'), ('11111111-1111-1111-1111-111111111111','bob','bob-1'),
  ('22222222-2222-2222-2222-222222222222','zoe','zoe-2');
INSERT INTO notes.note_items (owner_id, note_id, name) SELECT owner_id, id, 'item-'||editor FROM notes.notes;
INSERT INTO notes.note_events VALUES ('11111111-1111-1111-1111-111111111111','2026-03-01','a'),
  ('22222222-2222-2222-2222-222222222222','2026-03-02','b');
INSERT INTO notes.note_kinds (name) VALUES ('red');
INSERT INTO sites.site_readings (region, value) VALUES (1, 10), (2, 20);
INSERT INTO sites.site_archive (region, payload) VALUES (1, 'p1'), (2, 'p2');
INSERT INTO ledger.accounts VALUES (100, 'acc-100'), (200, 'acc-200');
INSERT INTO ledger.entries (account_id, amount) VALUES (100, 1), (200, 2);
