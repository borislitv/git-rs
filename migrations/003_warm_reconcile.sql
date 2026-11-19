-- Additive, serialized by the normal metadata migrator. Version 2 serving
-- clients can still use the source schema; this capability is opt-in by GRANT.
CREATE TABLE IF NOT EXISTS git_rs.warm_targets (
    worker name NOT NULL,
    slot integer NOT NULL CHECK (slot BETWEEN 1 AND 64),
    repo_id uuid NOT NULL REFERENCES git_rs.repositories ON DELETE CASCADE,
    principal text NOT NULL CHECK (principal ~ '^[A-Za-z0-9_.@-]{1,128}$'),
    branch bytea NOT NULL CHECK (octet_length(branch) BETWEEN 12 AND 1024
        AND substring(branch FOR 11) = decode('726566732f68656164732f','hex')),
    path bytea NOT NULL CHECK (octet_length(path) BETWEEN 1 AND 4096),
    PRIMARY KEY (worker, slot)
);

-- Independent of the target row lifetime: revocation must not remove an
-- in-progress reader's storage fence. Normal expiry/cleanup cascades here.
CREATE TABLE IF NOT EXISTS git_rs.warm_reader_holds (
    lease_id uuid PRIMARY KEY REFERENCES git_rs.repository_leases ON DELETE CASCADE,
    worker name NOT NULL,
    slot integer NOT NULL,
    principal text NOT NULL,
    branch bytea NOT NULL,
    path bytea NOT NULL
);
CREATE INDEX IF NOT EXISTS warm_reader_holds_target ON git_rs.warm_reader_holds(worker,slot);
REVOKE ALL ON git_rs.warm_targets, git_rs.warm_reader_holds FROM PUBLIC;

CREATE OR REPLACE VIEW git_rs.warm_live_scope AS
SELECT l.id, l.repo_id, r.generation, r.storage_epoch, h.principal, h.branch, h.path, f.oid
FROM git_rs.warm_reader_holds h
JOIN git_rs.repository_leases l ON l.id=h.lease_id
JOIN git_rs.warm_targets t ON t.worker=h.worker AND t.slot=h.slot AND t.repo_id=l.repo_id
    AND t.principal=h.principal AND t.branch=h.branch AND t.path=h.path
JOIN git_rs.repositories r ON r.id=l.repo_id
JOIN git_rs.permissions p ON p.repo_id=r.id AND p.principal=h.principal
JOIN git_rs.refs f ON f.repo_id=r.id AND f.name=h.branch
WHERE h.worker=session_user AND NOT l.exclusive AND l.expires_at>clock_timestamp();
REVOKE ALL ON git_rs.warm_live_scope FROM PUBLIC;

CREATE OR REPLACE FUNCTION git_rs.warm_slots() RETURNS SETOF integer
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT slot FROM git_rs.warm_targets WHERE worker=session_user ORDER BY slot;
$$;

CREATE OR REPLACE FUNCTION git_rs.warm_acquire(p_slot integer, p_lease uuid, p_seconds integer) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE wanted uuid; locked uuid; target git_rs.warm_targets%ROWTYPE;
BEGIN
    IF p_seconds IS NULL OR p_seconds NOT BETWEEN 30 AND 330 OR p_lease IS NULL THEN
        RAISE EXCEPTION 'invalid warm reader hold';
    END IF;
    SELECT repo_id INTO wanted FROM git_rs.warm_targets WHERE worker=session_user AND slot=p_slot;
    IF NOT FOUND THEN RETURN NULL; END IF;
    -- Same repository serialization as exclusive maintenance and source ACL
    -- changes. Do not wait behind publication or an in-progress maintenance lock.
    SELECT id INTO locked FROM git_rs.repositories WHERE id=wanted FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT t.* INTO target FROM git_rs.warm_targets t
    JOIN git_rs.permissions p ON p.repo_id=t.repo_id AND p.principal=t.principal
    JOIN git_rs.refs f ON f.repo_id=t.repo_id AND f.name=t.branch
    WHERE t.worker=session_user AND t.slot=p_slot AND t.repo_id=locked;
    IF NOT FOUND THEN RETURN NULL; END IF;
    IF EXISTS (SELECT 1 FROM git_rs.repository_leases l WHERE l.repo_id=locked
        AND l.exclusive AND l.expires_at>clock_timestamp()) OR EXISTS (
        SELECT 1 FROM git_rs.warm_reader_holds h JOIN git_rs.repository_leases l ON l.id=h.lease_id
        WHERE h.worker=session_user AND h.slot=p_slot AND l.expires_at>clock_timestamp()
    ) THEN RETURN NULL; END IF;
    INSERT INTO git_rs.repository_leases(id,repo_id,exclusive,expires_at)
    VALUES (p_lease,locked,false,clock_timestamp()+make_interval(secs=>p_seconds));
    INSERT INTO git_rs.warm_reader_holds VALUES (p_lease,session_user,p_slot,target.principal,target.branch,target.path);
    RETURN locked;
END;
$$;

CREATE OR REPLACE FUNCTION git_rs.warm_identity(p_lease uuid) RETURNS TABLE(principal text, path64 text)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT principal, replace(encode(path,'base64'),chr(10),'')
    FROM git_rs.warm_live_scope WHERE id=p_lease;
$$;

CREATE OR REPLACE FUNCTION git_rs.warm_snapshot(p_lease uuid) RETURNS SETOF text
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    WITH scope AS MATERIALIZED (SELECT * FROM git_rs.warm_live_scope WHERE id=p_lease)
    SELECT line_text FROM (
        SELECT 0 AS kind, concat_ws(chr(9),'repo',repo_id,generation,encode(branch,'hex'),storage_epoch) AS line_text FROM scope
        UNION ALL
        SELECT 1, concat_ws(chr(9),'ref',encode(branch,'hex'),encode(oid,'hex')) FROM scope
        UNION ALL
        SELECT 2, concat_ws(chr(9),'pack',q.native_hash,q.pack_key,q.idx_key,q.pack_digest,q.idx_digest,q.pack_size,q.idx_size)
        FROM scope s CROSS JOIN LATERAL (
            SELECT * FROM git_rs.packs WHERE repo_id=s.repo_id ORDER BY native_hash LIMIT 4097
        ) q
    ) framed ORDER BY kind;
$$;

CREATE OR REPLACE FUNCTION git_rs.warm_check(p_lease uuid,p_repo uuid,p_generation bigint,p_epoch bigint,p_tip bytea)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT EXISTS (SELECT 1 FROM git_rs.warm_live_scope WHERE id=p_lease AND repo_id=p_repo
        AND generation=p_generation AND storage_epoch=p_epoch AND oid=p_tip);
$$;

CREATE OR REPLACE FUNCTION git_rs.warm_release(p_lease uuid) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    DELETE FROM git_rs.repository_leases l USING git_rs.warm_reader_holds h
    WHERE l.id=p_lease AND h.lease_id=l.id AND h.worker=session_user AND NOT l.exclusive;
$$;

REVOKE ALL ON FUNCTION git_rs.warm_slots(), git_rs.warm_acquire(integer,uuid,integer),
    git_rs.warm_identity(uuid), git_rs.warm_snapshot(uuid),
    git_rs.warm_check(uuid,uuid,bigint,bigint,bytea), git_rs.warm_release(uuid) FROM PUBLIC;
