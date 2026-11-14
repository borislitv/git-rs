BEGIN;
SELECT pg_advisory_xact_lock(91079170002);
CREATE SCHEMA IF NOT EXISTS git_rs_cache;
REVOKE ALL ON SCHEMA git_rs_cache FROM PUBLIC;
SET LOCAL search_path = git_rs_cache, pg_catalog;
CREATE TABLE IF NOT EXISTS schema_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL CHECK (version = 1)
);
INSERT INTO schema_version VALUES (true, 1) ON CONFLICT DO NOTHING;
DO $$ BEGIN
    IF (SELECT count(*) FROM schema_version) <> 1
       OR (SELECT version FROM schema_version) <> 1 THEN
        RAISE EXCEPTION 'unsupported cache schema';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS namespaces (
    id uuid PRIMARY KEY,
    owner uuid NOT NULL,
    owner_role name NOT NULL DEFAULT current_user,
    prefix text NOT NULL UNIQUE CHECK (prefix = 'cache/' || id::text || '/'),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'quiescing', 'retired')),
    max_rows integer NOT NULL CHECK (max_rows BETWEEN 16 AND 1000000),
    max_bytes bigint NOT NULL CHECK (max_bytes BETWEEN 1 AND 1099511627776),
    used_rows integer NOT NULL DEFAULT 0 CHECK (used_rows >= 0 AND used_rows <= max_rows),
    used_bytes bigint NOT NULL DEFAULT 0 CHECK (used_bytes >= 0 AND used_bytes <= max_bytes)
);
CREATE TABLE IF NOT EXISTS jobs (
    namespace uuid NOT NULL REFERENCES namespaces(id),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    canonical bytea NOT NULL CHECK (octet_length(canonical) BETWEEN 1 AND 8192),
    token uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    lease_until timestamptz NOT NULL,
    hard_until timestamptz NOT NULL,
    state text NOT NULL CHECK (state IN ('computing', 'ready')),
    staged_payload uuid,
    PRIMARY KEY (namespace, key_hash)
);
CREATE TABLE IF NOT EXISTS payloads (
    namespace uuid NOT NULL REFERENCES namespaces(id),
    id uuid NOT NULL,
    key_hash bytea NOT NULL,
    token uuid NOT NULL,
    generation bigint NOT NULL,
    object_key text CHECK (length(object_key) <= 128),
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    size bigint NOT NULL CHECK (size BETWEEN 0 AND 8388608),
    body bytea CHECK (octet_length(body) <= 8388608),
    state text NOT NULL CHECK (state IN ('staged', 'live', 'retired')),
    retired_at timestamptz,
    delete_after timestamptz,
    last_delete_at timestamptz,
    delete_attempts bigint NOT NULL DEFAULT 0 CHECK (delete_attempts >= 0),
    PRIMARY KEY (namespace, id),
    UNIQUE (object_key),
    CHECK (object_key IS NULL OR object_key = 'cache/' || namespace::text || '/' || id::text),
    CHECK (body IS NULL OR (object_key IS NULL AND octet_length(body) = size)),
    CHECK ((state = 'retired') = (retired_at IS NOT NULL AND delete_after IS NOT NULL)),
    FOREIGN KEY (namespace, key_hash) REFERENCES jobs(namespace, key_hash)
);
CREATE TABLE IF NOT EXISTS directory (
    namespace uuid NOT NULL,
    key_hash bytea NOT NULL,
    payload uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('ready', 'expired')),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, key_hash),
    FOREIGN KEY (namespace, key_hash) REFERENCES jobs(namespace, key_hash),
    FOREIGN KEY (namespace, payload) REFERENCES payloads(namespace, id)
);
CREATE INDEX IF NOT EXISTS directory_expiry ON directory(namespace, expires_at, key_hash) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS directory_payload ON directory(namespace, payload);
CREATE TABLE IF NOT EXISTS read_holds (
    namespace uuid NOT NULL,
    reader uuid NOT NULL,
    payload uuid NOT NULL,
    lease_until timestamptz NOT NULL,
    PRIMARY KEY (namespace, reader),
    FOREIGN KEY (namespace, payload) REFERENCES payloads(namespace, id)
);
CREATE INDEX IF NOT EXISTS read_holds_expiry ON read_holds(namespace, lease_until, reader);
CREATE INDEX IF NOT EXISTS read_holds_payload ON read_holds(namespace, payload, lease_until);
CREATE TABLE IF NOT EXISTS upload_intents (
    namespace uuid NOT NULL,
    attempt uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('planned', 'confirmed', 'unknown', 'retired')),
    last_result text NOT NULL DEFAULT 'planned' CHECK (length(last_result) <= 64),
    reconciled_at timestamptz,
    PRIMARY KEY (namespace, attempt),
    FOREIGN KEY (namespace, attempt) REFERENCES payloads(namespace, id)
);
CREATE INDEX IF NOT EXISTS pending_intents ON upload_intents(namespace, attempt) WHERE state IN ('planned', 'unknown', 'confirmed');
CREATE INDEX IF NOT EXISTS retired_payloads ON payloads(namespace, delete_after, id) WHERE state = 'retired';
CREATE TABLE IF NOT EXISTS maintenance_claims (
    namespace uuid PRIMARY KEY REFERENCES namespaces(id),
    token uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    lease_until timestamptz NOT NULL
);

-- Every mutation and read admission locks namespace first. The short lock makes
-- quota reservations, expiry-after-lock-wait and reader/retirement exclusion atomic.
CREATE OR REPLACE FUNCTION guard(n uuid, o uuid, active boolean DEFAULT true) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE ns namespaces;
BEGIN
    SELECT * INTO STRICT ns FROM namespaces WHERE id = n FOR UPDATE;
    IF ns.owner <> o OR ns.owner_role <> current_user OR (active AND ns.state <> 'active') THEN
        RAISE EXCEPTION 'cache namespace not owned or active';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION charge(n uuid, r integer, b bigint) RETURNS void
LANGUAGE plpgsql AS $$ BEGIN
    UPDATE namespaces SET used_rows = used_rows + r, used_bytes = used_bytes + b
      WHERE id = n AND used_rows + r BETWEEN 0 AND max_rows AND used_bytes + b BETWEEN 0 AND max_bytes;
    IF NOT FOUND THEN RAISE EXCEPTION 'cache namespace quota exceeded'; END IF;
END $$;

CREATE OR REPLACE FUNCTION register_namespace(n uuid, o uuid, r integer, b bigint) RETURNS void
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM pg_advisory_xact_lock(91079170002);
    IF NOT EXISTS (SELECT 1 FROM namespaces WHERE id = n) THEN
        IF (SELECT count(*) FROM namespaces) >= 1024 THEN RAISE EXCEPTION 'cache namespace count limit exceeded'; END IF;
        INSERT INTO namespaces(id, owner, prefix, max_rows, max_bytes) VALUES(n, o, 'cache/' || n::text || '/', r, b);
    END IF;
    PERFORM guard(n, o);
    IF NOT EXISTS (SELECT 1 FROM namespaces WHERE id = n AND max_rows = r AND max_bytes = b) THEN
        RAISE EXCEPTION 'cache namespace configuration mismatch';
    END IF;
    -- This fixed authority slot is separate from the artifact-row budget.
    -- A completely full namespace must still be able to run reclamation. Never
    -- reset an existing token/generation when the migrator is run again.
    INSERT INTO maintenance_claims(namespace, token, generation, lease_until)
        VALUES(n, o, 1, '-infinity') ON CONFLICT DO NOTHING;
END $$;

CREATE OR REPLACE FUNCTION claim(n uuid, o uuid, k bytea, c bytea, t uuid, seconds integer, hard_seconds integer)
RETURNS TABLE(owned boolean, attempt bigint) LANGUAGE plpgsql AS $$
DECLARE j jobs; now_at timestamptz;
BEGIN
    PERFORM guard(n, o);
    IF seconds NOT BETWEEN 1 AND 60 OR hard_seconds NOT BETWEEN seconds AND 120 THEN
        RAISE EXCEPTION 'invalid cache job lifetime';
    END IF;
    SELECT * INTO j FROM jobs WHERE namespace = n AND key_hash = k FOR UPDATE;
    now_at := clock_timestamp();
    IF FOUND THEN
        IF j.canonical <> c THEN RAISE EXCEPTION 'cache key collision'; END IF;
        IF j.token = t THEN
            RETURN QUERY SELECT j.state = 'computing' AND j.lease_until > now_at AND j.hard_until > now_at, j.generation;
            RETURN;
        END IF;
        IF (j.state = 'computing' AND j.lease_until > now_at)
            OR EXISTS (SELECT 1 FROM directory WHERE namespace = n AND key_hash = k AND state = 'ready' AND expires_at > now_at) THEN
            RETURN QUERY SELECT false, j.generation;
            RETURN;
        END IF;
        UPDATE jobs SET token = t, generation = generation + 1, state = 'computing', staged_payload = NULL,
            lease_until = now_at + make_interval(secs => seconds), hard_until = now_at + make_interval(secs => hard_seconds)
          WHERE namespace = n AND key_hash = k RETURNING * INTO j;
    ELSE
        PERFORM charge(n, 1, 0);
        INSERT INTO jobs(namespace, key_hash, canonical, token, generation, lease_until, hard_until, state)
          VALUES(n, k, c, t, 1, now_at + make_interval(secs => seconds), now_at + make_interval(secs => hard_seconds), 'computing') RETURNING * INTO j;
    END IF;
    RETURN QUERY SELECT true, j.generation;
END $$;

CREATE OR REPLACE FUNCTION valid_job(n uuid, k bytea, t uuid, g bigint) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE j jobs;
BEGIN
    SELECT * INTO j FROM jobs WHERE namespace = n AND key_hash = k FOR UPDATE;
    RETURN FOUND AND j.token = t AND j.generation = g AND j.state = 'computing'
        AND j.lease_until > clock_timestamp() AND j.hard_until > clock_timestamp();
END $$;

CREATE OR REPLACE FUNCTION renew(n uuid, o uuid, k bytea, t uuid, g bigint, seconds integer) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n, o);
    IF seconds NOT BETWEEN 1 AND 60 THEN RAISE EXCEPTION 'invalid cache renewal'; END IF;
    IF NOT valid_job(n, k, t, g) THEN RETURN false; END IF;
    UPDATE jobs SET lease_until = least(hard_until, clock_timestamp() + make_interval(secs => seconds))
      WHERE namespace = n AND key_hash = k;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION prepare_payload(n uuid, o uuid, k bytea, t uuid, g bigint, p uuid, h bytea, s bigint, external boolean)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE previous uuid;
BEGIN
    PERFORM guard(n, o);
    IF NOT valid_job(n, k, t, g) THEN RETURN false; END IF;
    SELECT staged_payload INTO previous FROM jobs WHERE namespace = n AND key_hash = k;
    IF previous IS NOT NULL THEN
        RETURN previous = p AND EXISTS(SELECT 1 FROM payloads WHERE namespace = n AND id = p AND digest = h AND size = s
            AND (object_key IS NOT NULL) = external AND state = 'staged');
    END IF;
    PERFORM charge(n, CASE WHEN external THEN 2 ELSE 1 END, s);
    INSERT INTO payloads(namespace, id, key_hash, token, generation, object_key, digest, size, state)
        VALUES(n, p, k, t, g, CASE WHEN external THEN 'cache/' || n::text || '/' || p::text END, h, s, 'staged');
    IF external THEN INSERT INTO upload_intents(namespace, attempt, state) VALUES(n, p, 'planned'); END IF;
    UPDATE jobs SET staged_payload = p WHERE namespace = n AND key_hash = k;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION upload_result(n uuid, o uuid, p uuid, confirmed boolean) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n, o, false);
    UPDATE upload_intents SET state = CASE WHEN state = 'retired' THEN state WHEN confirmed THEN 'confirmed' ELSE 'unknown' END,
        last_result = CASE WHEN confirmed THEN 'PUT confirmed' ELSE 'PUT outcome unknown' END, reconciled_at = clock_timestamp()
      WHERE namespace = n AND attempt = p;
    RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION publish(n uuid, o uuid, k bytea, t uuid, g bigint, p uuid, value bytea, seconds integer)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE v payloads;
BEGIN
    PERFORM guard(n, o);
    IF seconds NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid cache TTL'; END IF;
    IF NOT valid_job(n, k, t, g) THEN RETURN false; END IF;
    SELECT * INTO v FROM payloads WHERE namespace = n AND id = p FOR UPDATE;
    IF NOT FOUND OR v.state <> 'staged' OR v.key_hash <> k OR v.token <> t OR v.generation <> g THEN RETURN false; END IF;
    PERFORM 1 FROM directory WHERE namespace = n AND key_hash = k FOR UPDATE;
    IF NOT valid_job(n, k, t, g) THEN RETURN false; END IF;
    IF v.object_key IS NULL THEN
        IF value IS NULL OR octet_length(value) <> v.size THEN RAISE EXCEPTION 'invalid inline cache payload'; END IF;
    ELSIF value IS NOT NULL OR NOT EXISTS(SELECT 1 FROM upload_intents WHERE namespace = n AND attempt = p AND state = 'confirmed') THEN
        RETURN false;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM directory WHERE namespace = n AND key_hash = k) THEN PERFORM charge(n, 1, 0); END IF;
    UPDATE payloads SET state = 'live', body = value WHERE namespace = n AND id = p;
    INSERT INTO directory(namespace, key_hash, payload, state, expires_at)
        VALUES(n, k, p, 'ready', clock_timestamp() + make_interval(secs => seconds))
        ON CONFLICT (namespace, key_hash) DO UPDATE SET payload = excluded.payload, state = 'ready', expires_at = excluded.expires_at;
    UPDATE jobs SET state = 'ready' WHERE namespace = n AND key_hash = k;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION acquire_read(n uuid, o uuid, k bytea, c bytea, r uuid, seconds integer, max_size bigint)
RETURNS TABLE(payload text, digest bytea, size bigint, object_key text, lifetime_ms bigint)
LANGUAGE plpgsql AS $$
DECLARE v payloads; expiry timestamptz;
BEGIN
    PERFORM guard(n, o);
    IF seconds NOT BETWEEN 1 AND 120 OR max_size NOT BETWEEN 1 AND 8388608 THEN RAISE EXCEPTION 'invalid cache read limits'; END IF;
    SELECT p.* INTO v FROM directory d JOIN jobs j USING(namespace, key_hash)
        JOIN payloads p ON p.namespace = d.namespace AND p.id = d.payload
        WHERE d.namespace = n AND d.key_hash = k AND j.canonical = c AND d.state = 'ready'
        AND d.expires_at > clock_timestamp() AND p.state = 'live' AND p.size <= max_size;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n, 1, 0);
    expiry := clock_timestamp() + make_interval(secs => seconds);
    INSERT INTO read_holds VALUES(n, r, v.id, expiry);
    RETURN QUERY SELECT v.id::text, v.digest, v.size, v.object_key,
        floor(extract(epoch FROM (expiry - clock_timestamp())) * 1000)::bigint;
END $$;

CREATE OR REPLACE FUNCTION release_read(n uuid, o uuid, r uuid) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n, o, false);
    DELETE FROM read_holds WHERE namespace = n AND reader = r;
    IF NOT FOUND THEN RETURN false; END IF;
    PERFORM charge(n, -1, 0);
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION invalidate(n uuid, o uuid, k bytea, p uuid) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n, o);
    UPDATE directory SET state = 'expired' WHERE namespace = n AND key_hash = k AND payload = p AND state = 'ready';
    RETURN FOUND;
END $$;

CREATE OR REPLACE FUNCTION claim_maintenance(n uuid, o uuid, t uuid, seconds integer)
RETURNS TABLE(owned boolean, attempt bigint) LANGUAGE plpgsql AS $$
DECLARE m maintenance_claims;
BEGIN
    PERFORM guard(n, o, false);
    IF seconds NOT BETWEEN 1 AND 60 THEN RAISE EXCEPTION 'invalid cache maintenance lifetime'; END IF;
    SELECT * INTO m FROM maintenance_claims WHERE namespace = n FOR UPDATE;
    IF FOUND THEN
        IF m.token = t THEN RETURN QUERY SELECT m.lease_until > clock_timestamp(), m.generation; RETURN; END IF;
        IF m.lease_until > clock_timestamp() THEN RETURN QUERY SELECT false, m.generation; RETURN; END IF;
        UPDATE maintenance_claims SET token = t, generation = generation + 1, lease_until = clock_timestamp() + make_interval(secs => seconds)
            WHERE namespace = n RETURNING * INTO m;
    ELSE
        -- Compatibility with a namespace registered before the reserved slot
        -- was introduced. Its artifact quota must not block this singleton.
        INSERT INTO maintenance_claims VALUES(n, t, 1, clock_timestamp() + make_interval(secs => seconds)) RETURNING * INTO m;
    END IF;
    RETURN QUERY SELECT true, m.generation;
END $$;

CREATE OR REPLACE FUNCTION guard_maintenance(n uuid, o uuid, t uuid, g bigint) RETURNS void
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n, o, false);
    IF NOT EXISTS(SELECT 1 FROM maintenance_claims WHERE namespace = n AND token = t AND generation = g AND lease_until > clock_timestamp()) THEN
        RAISE EXCEPTION 'expired cache maintenance authority';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION expire_batch(n uuid, o uuid, t uuid, g bigint, amount integer) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE expired bigint; released integer;
BEGIN
    PERFORM guard_maintenance(n, o, t, g);
    IF amount NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid cache batch'; END IF;
    UPDATE directory SET state = 'expired' WHERE namespace = n AND key_hash IN (
        SELECT key_hash FROM directory WHERE namespace = n AND state = 'ready' AND expires_at <= clock_timestamp() ORDER BY expires_at, key_hash LIMIT amount);
    GET DIAGNOSTICS expired = ROW_COUNT;
    DELETE FROM read_holds WHERE namespace = n AND reader IN (
        SELECT reader FROM read_holds WHERE namespace = n AND lease_until <= clock_timestamp() ORDER BY lease_until, reader LIMIT amount);
    GET DIAGNOSTICS released = ROW_COUNT;
    PERFORM charge(n, -released, 0);
    RETURN expired;
END $$;

CREATE OR REPLACE FUNCTION retire_payloads(n uuid, o uuid, t uuid, g bigint, amount integer, grace integer, intents_only boolean)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE retired bigint;
BEGIN
    PERFORM guard_maintenance(n, o, t, g);
    IF amount NOT BETWEEN 1 AND 100 OR grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid cache retirement limits'; END IF;
    WITH changed AS (UPDATE payloads SET state = 'retired', retired_at = clock_timestamp(), delete_after = clock_timestamp() + make_interval(secs => grace)
      WHERE namespace = n AND id IN (
        SELECT p.id FROM payloads p WHERE p.namespace = n AND p.state <> 'retired'
          AND (NOT intents_only OR p.state = 'staged')
          AND NOT EXISTS(SELECT 1 FROM directory d WHERE d.namespace = n AND d.payload = p.id AND d.state = 'ready' AND d.expires_at > clock_timestamp())
          AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.namespace = n AND j.staged_payload = p.id AND j.state = 'computing' AND j.lease_until > clock_timestamp() AND j.hard_until > clock_timestamp())
          ORDER BY p.id LIMIT amount) RETURNING id),
    intents AS (UPDATE upload_intents i SET state = 'retired', last_result = 'retired; reconcile late PUT', reconciled_at = clock_timestamp()
      WHERE i.namespace = n AND i.attempt IN (SELECT id FROM changed) AND i.state <> 'retired' RETURNING attempt)
    SELECT count(*) INTO retired FROM changed;
    RETURN retired;
END $$;

CREATE OR REPLACE FUNCTION deletion_batch(n uuid, o uuid, t uuid, g bigint, amount integer)
RETURNS TABLE(payload text, object_key text) LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard_maintenance(n, o, t, g);
    IF amount NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid cache deletion batch'; END IF;
    RETURN QUERY SELECT p.id::text, p.object_key FROM payloads p WHERE p.namespace = n AND p.state = 'retired'
        AND p.delete_after <= clock_timestamp()
        AND (p.last_delete_at IS NULL OR (p.object_key IS NOT NULL AND p.last_delete_at < clock_timestamp() - interval '30 seconds'))
        AND NOT EXISTS(SELECT 1 FROM read_holds h WHERE h.namespace = n AND h.payload = p.id AND h.lease_until > clock_timestamp())
        ORDER BY p.last_delete_at NULLS FIRST, p.id LIMIT amount;
END $$;

CREATE OR REPLACE FUNCTION record_delete(n uuid, o uuid, t uuid, g bigint, p uuid) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE v payloads;
BEGIN
    PERFORM guard_maintenance(n, o, t, g);
    SELECT * INTO STRICT v FROM payloads WHERE namespace = n AND id = p AND state = 'retired' AND delete_after <= clock_timestamp();
    IF EXISTS(SELECT 1 FROM read_holds WHERE namespace = n AND payload = p AND lease_until > clock_timestamp()) THEN
        RAISE EXCEPTION 'cache payload still held';
    END IF;
    IF v.object_key IS NULL AND v.last_delete_at IS NULL THEN PERFORM charge(n, 0, -v.size); END IF;
    UPDATE payloads SET body = NULL, last_delete_at = clock_timestamp(), delete_attempts = delete_attempts + 1 WHERE namespace = n AND id = p;
    -- Retain immutable identity, intents and S3 reservations. A delayed unknown
    -- PUT can recreate the object after DELETE; tombstones remain rediscoverable.
END $$;

REVOKE ALL ON ALL TABLES IN SCHEMA git_rs_cache FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA git_rs_cache FROM PUBLIC;
COMMIT;
