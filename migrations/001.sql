-- Run by `git-rs migrate` inside its serialized transaction.
CREATE SCHEMA IF NOT EXISTS git_rs;
SET LOCAL search_path = git_rs, pg_catalog;
DO $initialize$ BEGIN
IF to_regclass('git_rs.schema_version') IS NULL THEN
CREATE TABLE schema_version (version integer PRIMARY KEY CHECK (version = 1));
INSERT INTO schema_version VALUES (1);
CREATE TABLE repositories (
    id uuid PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    default_ref bytea NOT NULL DEFAULT convert_to('refs/heads/main', 'UTF8'),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    storage_epoch bigint NOT NULL DEFAULT 0 CHECK (storage_epoch >= 0)
);
CREATE TABLE permissions (
    repo_id uuid REFERENCES repositories ON DELETE CASCADE,
    principal text NOT NULL,
    role text NOT NULL CHECK (role IN ('reader', 'writer', 'maintainer', 'owner')),
    write_prefix bytea NOT NULL DEFAULT convert_to('refs/', 'UTF8'),
    PRIMARY KEY (repo_id, principal)
);
CREATE TABLE refs (
    repo_id uuid REFERENCES repositories ON DELETE CASCADE,
    name bytea NOT NULL,
    oid bytea NOT NULL CHECK (octet_length(oid) = 20),
    PRIMARY KEY (repo_id, name)
);
CREATE TABLE packs (
    repo_id uuid REFERENCES repositories ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    native_hash text NOT NULL CHECK (native_hash ~ '^[0-9a-f]{40}$'),
    pack_key text NOT NULL UNIQUE,
    idx_key text NOT NULL UNIQUE,
    pack_digest text NOT NULL CHECK (pack_digest ~ '^[0-9a-f]{64}$'),
    idx_digest text NOT NULL CHECK (idx_digest ~ '^[0-9a-f]{64}$'),
    pack_size bigint NOT NULL CHECK (pack_size > 0),
    idx_size bigint NOT NULL CHECK (idx_size > 0),
    generation bigint NOT NULL,
    PRIMARY KEY (repo_id, native_hash)
);
CREATE TABLE operations (
    id uuid PRIMARY KEY,
    repo_id uuid REFERENCES repositories,
    principal text NOT NULL,
    commands bytea NOT NULL,
    generation bigint NOT NULL,
    confirmations bigint NOT NULL DEFAULT 0,
    committed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE ref_updates (
    operation_id uuid REFERENCES operations,
    ordinal integer NOT NULL,
    name bytea NOT NULL,
    old_oid bytea,
    new_oid bytea,
    PRIMARY KEY (operation_id, ordinal)
);
CREATE TABLE retired_objects (
    repo_id uuid REFERENCES repositories,
    object_key text PRIMARY KEY,
    retired_epoch bigint NOT NULL
);
CREATE TABLE staged_objects (
    repo_id uuid REFERENCES repositories,
    operation_id uuid NOT NULL,
    object_key text PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('planned', 'uploaded')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE repository_leases (
    id uuid PRIMARY KEY,
    repo_id uuid NOT NULL REFERENCES repositories,
    exclusive boolean NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX ON repository_leases (repo_id, expires_at);
ELSIF NOT (SELECT count(*)=1 AND min(version)=1 FROM schema_version) THEN
    RAISE EXCEPTION 'unsupported metadata schema version';
END IF;
END $initialize$;

-- Refresh the implementation for initialized version-1 databases as well.
CREATE OR REPLACE FUNCTION publish(
    p_repo uuid, p_operation uuid, p_principal text, p_commands bytea, p_epoch bigint, p_lease uuid,
    p_names bytea[], p_old bytea[], p_new bytea[], p_fast_forward boolean[]
) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    r repositories%ROWTYPE;
    permission permissions%ROWTYPE;
    previous operations%ROWTYPE;
    current_oid bytea;
    zero_oid bytea := decode(repeat('00', 20), 'hex');
    i integer;
    g bigint;
BEGIN
    SELECT * INTO STRICT r FROM repositories WHERE id = p_repo FOR UPDATE;
    IF NOT EXISTS (SELECT 1 FROM repository_leases WHERE id = p_lease AND repo_id = p_repo
                   AND NOT exclusive AND expires_at > clock_timestamp())
    THEN RAISE EXCEPTION 'request lease unavailable'; END IF;
    IF r.storage_epoch <> p_epoch THEN RAISE EXCEPTION 'storage epoch changed'; END IF;
    SELECT * INTO previous FROM operations WHERE id = p_operation;
    IF FOUND THEN
        IF previous.repo_id <> p_repo OR previous.principal <> p_principal OR previous.commands <> p_commands THEN
            RAISE EXCEPTION 'operation identity conflict';
        END IF;
        -- A WAL-producing synchronous confirmation, also used after unknown COMMIT.
        UPDATE operations SET confirmations = confirmations + 1 WHERE id = p_operation;
        RETURN previous.generation;
    END IF;
    SELECT * INTO permission FROM permissions WHERE repo_id = p_repo AND principal = p_principal;
    IF NOT FOUND OR permission.role = 'reader' THEN RAISE EXCEPTION 'write denied'; END IF;
    IF cardinality(p_names) NOT BETWEEN 1 AND 10000 OR cardinality(p_old) <> cardinality(p_names)
       OR cardinality(p_new) <> cardinality(p_names) OR cardinality(p_fast_forward) <> cardinality(p_names)
       OR EXISTS (SELECT 1 FROM unnest(p_names) n GROUP BY n HAVING count(*) > 1)
    THEN RAISE EXCEPTION 'invalid command list'; END IF;
    FOR i IN 1..cardinality(p_names) LOOP
        IF substring(p_names[i] FROM 1 FOR octet_length(permission.write_prefix)) <> permission.write_prefix
        THEN RAISE EXCEPTION 'ref write denied'; END IF;
        SELECT oid INTO current_oid FROM refs WHERE repo_id = p_repo AND name = p_names[i];
        IF coalesce(current_oid, zero_oid) <> p_old[i] THEN RAISE EXCEPTION 'stale old OID'; END IF;
        IF p_names[i] = r.default_ref AND (p_new[i] = zero_oid OR NOT p_fast_forward[i])
        THEN RAISE EXCEPTION 'default branch protected'; END IF;
        IF permission.role = 'writer' AND
           (p_new[i] = zero_oid OR NOT p_fast_forward[i] OR
            (substring(p_names[i] FROM 1 FOR 10) = convert_to('refs/tags/', 'UTF8') AND p_old[i] <> zero_oid))
        THEN RAISE EXCEPTION 'maintainer permission required'; END IF;
    END LOOP;
    g := r.generation + 1;
    INSERT INTO operations(id, repo_id, principal, commands, generation)
        VALUES(p_operation, p_repo, p_principal, p_commands, g);
    FOR i IN 1..cardinality(p_names) LOOP
        DELETE FROM refs WHERE repo_id = p_repo AND name = p_names[i];
        IF p_new[i] <> zero_oid THEN
            INSERT INTO refs VALUES(p_repo, p_names[i], p_new[i]);
        END IF;
        INSERT INTO ref_updates VALUES(p_operation, i, p_names[i], nullif(p_old[i], zero_oid), nullif(p_new[i], zero_oid));
    END LOOP;
    -- Validate the complete post-image while holding the same repository lock.
    IF EXISTS (
        -- A descendant lies in [name || '/', name || '0') in bytea order.
        -- Both bounds use the refs primary key rather than a quadratic scan
        -- that exceeds statement_timeout for otherwise legal long ref names.
        SELECT 1 FROM refs a CROSS JOIN LATERAL (
            SELECT 1 FROM refs b WHERE b.repo_id = a.repo_id
            AND b.name >= a.name || decode('2f', 'hex')
            AND b.name < a.name || decode('30', 'hex')
            ORDER BY b.name LIMIT 1
        ) descendant WHERE a.repo_id = p_repo
    ) THEN RAISE EXCEPTION 'ref namespace conflict'; END IF;
    IF (SELECT count(*) FROM refs WHERE repo_id = p_repo) > 10000 THEN RAISE EXCEPTION 'ref limit exceeded'; END IF;
    UPDATE repositories SET generation = g WHERE id = p_repo;
    RETURN g;
END $$;
