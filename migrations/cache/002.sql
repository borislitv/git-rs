-- Forward-only optional-cache upgrade. Run through cache-migrate with the cache
-- role, not source migrate. A failed transaction leaves v1 unchanged. After a
-- successful v2 upgrade old cache-migrate binaries deliberately reject version 2;
-- rollback requires an old binary with caching disabled or a fresh namespace/DB,
-- never deleting/reusing old payload identities or guessing that old writers died.
BEGIN;
SELECT pg_advisory_xact_lock(91079170002);
SET LOCAL search_path = git_rs_cache, pg_catalog;
DO $$ BEGIN
    IF (SELECT version FROM schema_version WHERE singleton) <> 1 THEN
        RAISE EXCEPTION 'cache v2 migration requires v1';
    END IF;
END $$;

CREATE TABLE blame_builds (
    namespace uuid NOT NULL REFERENCES namespaces(id),
    key_hash bytea NOT NULL CHECK (octet_length(key_hash) = 32),
    canonical bytea NOT NULL CHECK (octet_length(canonical) BETWEEN 1 AND 8192),
    protocol integer NOT NULL DEFAULT 1 CHECK (protocol = 1),
    token uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    lease_until timestamptz NOT NULL,
    hard_until timestamptz NOT NULL,
    PRIMARY KEY(namespace, key_hash)
);
ALTER TABLE jobs ADD COLUMN build_key bytea;
ALTER TABLE jobs ADD COLUMN build_token uuid;
ALTER TABLE jobs ADD COLUMN build_generation bigint;
ALTER TABLE jobs ADD COLUMN checkpoint_write boolean NOT NULL DEFAULT false;
ALTER TABLE jobs ADD COLUMN checkpoint_previous uuid;
ALTER TABLE jobs ADD COLUMN checkpoint_digest bytea;
ALTER TABLE jobs ADD CONSTRAINT checkpoint_expected CHECK (
    (checkpoint_previous IS NULL AND checkpoint_digest IS NULL)
    OR (checkpoint_previous IS NOT NULL AND checkpoint_digest IS NOT NULL AND octet_length(checkpoint_digest)=32));
ALTER TABLE jobs ADD CONSTRAINT job_build_owner CHECK (
    (build_key IS NULL AND build_token IS NULL AND build_generation IS NULL)
    OR (build_key IS NOT NULL AND octet_length(build_key) = 32 AND build_token IS NOT NULL
        AND build_generation IS NOT NULL AND build_generation > 0));
ALTER TABLE jobs ADD FOREIGN KEY(namespace, build_key) REFERENCES blame_builds(namespace, key_hash);
-- Ordinary opaque takeover must not inherit an expired parent binding. Keep
-- the v1 claim API unchanged; a bound claim attaches its new parent afterward.
CREATE FUNCTION clear_job_build_owner() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.token<>OLD.token THEN
        NEW.build_key := NULL; NEW.build_token := NULL; NEW.build_generation := NULL;
        NEW.checkpoint_write := false; NEW.checkpoint_previous := NULL; NEW.checkpoint_digest := NULL;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER clear_job_build_owner BEFORE UPDATE OF token ON jobs
    FOR EACH ROW EXECUTE FUNCTION clear_job_build_owner();

CREATE TABLE build_chunks (
    namespace uuid NOT NULL,
    build_key bytea NOT NULL,
    token uuid NOT NULL,
    generation bigint NOT NULL,
    child uuid NOT NULL,
    PRIMARY KEY(namespace, build_key, token, generation, child),
    FOREIGN KEY(namespace, build_key) REFERENCES blame_builds(namespace, key_hash),
    FOREIGN KEY(namespace, child) REFERENCES payloads(namespace, id)
);
CREATE INDEX build_chunks_child ON build_chunks(namespace, child);
CREATE TABLE manifest_roots (
    namespace uuid NOT NULL,
    payload uuid NOT NULL,
    kind integer NOT NULL CHECK (kind BETWEEN 1 AND 3),
    children integer NOT NULL CHECK (children BETWEEN 0 AND 64),
    total_bytes bigint NOT NULL CHECK (total_bytes BETWEEN 0 AND 268435456),
    PRIMARY KEY(namespace, payload),
    FOREIGN KEY(namespace, payload) REFERENCES payloads(namespace, id)
);
CREATE TABLE manifest_children (
    namespace uuid NOT NULL,
    parent uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal BETWEEN 0 AND 63),
    child uuid NOT NULL,
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    size bigint NOT NULL CHECK (size BETWEEN 1 AND 4194304),
    PRIMARY KEY(namespace, parent, ordinal),
    CHECK (parent <> child),
    FOREIGN KEY(namespace, parent) REFERENCES manifest_roots(namespace, payload),
    FOREIGN KEY(namespace, child) REFERENCES payloads(namespace, id)
);
CREATE INDEX manifest_children_child ON manifest_children(namespace, child);

-- All callers first take guard's namespace lock. Capture DB time only after
-- subsequent row-lock waits; neither retries nor renewal move hard_until.
CREATE FUNCTION valid_blame_build(n uuid, k bytea, t uuid, g bigint) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE b blame_builds;
BEGIN
    SELECT * INTO b FROM blame_builds WHERE namespace=n AND key_hash=k FOR UPDATE;
    RETURN FOUND AND b.token=t AND b.generation=g AND b.lease_until>clock_timestamp() AND b.hard_until>clock_timestamp();
END $$;

CREATE FUNCTION claim_blame_build(n uuid, o uuid, k bytea, c bytea, t uuid, seconds integer, hard_seconds integer)
RETURNS TABLE(owned boolean, attempt bigint, remaining_ms bigint) LANGUAGE plpgsql AS $$
DECLARE b blame_builds; now_at timestamptz;
BEGIN
    PERFORM guard(n,o);
    IF hard_seconds NOT BETWEEN 1 AND 3600 OR seconds NOT BETWEEN 1 AND least(60,hard_seconds) THEN
        RAISE EXCEPTION 'invalid blame-build lifetime';
    END IF;
    SELECT * INTO b FROM blame_builds WHERE namespace=n AND key_hash=k FOR UPDATE;
    now_at := clock_timestamp();
    IF FOUND THEN
        IF b.canonical<>c THEN RAISE EXCEPTION 'blame-build key collision'; END IF;
        IF b.token=t THEN
            RETURN QUERY SELECT b.lease_until>now_at AND b.hard_until>now_at, b.generation,
                greatest(0,floor(extract(epoch FROM (b.hard_until-clock_timestamp()))*1000)::bigint);
            RETURN;
        END IF;
        IF b.lease_until>now_at AND b.hard_until>now_at THEN
            RETURN QUERY SELECT false,b.generation,0::bigint; RETURN;
        END IF;
        UPDATE blame_builds SET token=t,generation=generation+1,
            lease_until=now_at+make_interval(secs=>seconds),hard_until=now_at+make_interval(secs=>hard_seconds)
            WHERE namespace=n AND key_hash=k RETURNING * INTO b;
    ELSE
        PERFORM charge(n,1,0);
        INSERT INTO blame_builds(namespace,key_hash,canonical,token,generation,lease_until,hard_until)
            VALUES(n,k,c,t,1,now_at+make_interval(secs=>seconds),now_at+make_interval(secs=>hard_seconds)) RETURNING * INTO b;
    END IF;
    RETURN QUERY SELECT true,b.generation,
        greatest(0,floor(extract(epoch FROM (b.hard_until-clock_timestamp()))*1000)::bigint);
END $$;

CREATE FUNCTION check_blame_build(n uuid,o uuid,k bytea,t uuid,g bigint) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n,o); RETURN valid_blame_build(n,k,t,g);
END $$;
CREATE FUNCTION renew_blame_build(n uuid,o uuid,k bytea,t uuid,g bigint,seconds integer) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 60 THEN RAISE EXCEPTION 'invalid blame-build renewal'; END IF;
    IF NOT valid_blame_build(n,k,t,g) THEN RETURN false; END IF;
    UPDATE blame_builds SET lease_until=least(hard_until,clock_timestamp()+make_interval(secs=>seconds))
        WHERE namespace=n AND key_hash=k;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION valid_job(n uuid,k bytea,t uuid,g bigint) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE j jobs;
BEGIN
    SELECT * INTO j FROM jobs WHERE namespace=n AND key_hash=k FOR UPDATE;
    IF NOT FOUND OR j.token<>t OR j.generation<>g OR j.state<>'computing'
        OR j.lease_until<=clock_timestamp() OR j.hard_until<=clock_timestamp() THEN RETURN false; END IF;
    IF j.build_key IS NOT NULL AND NOT valid_blame_build(n,j.build_key,j.build_token,j.build_generation) THEN RETURN false; END IF;
    -- Parent authority may itself have waited on a row lock.
    RETURN j.lease_until>clock_timestamp() AND j.hard_until>clock_timestamp();
END $$;

CREATE FUNCTION claim_blame_payload(n uuid,o uuid,k bytea,c bytea,t uuid,seconds integer,hard_seconds integer,
    bk bytea,bt uuid,bg bigint) RETURNS TABLE(owned boolean,attempt bigint) LANGUAGE plpgsql AS $$
DECLARE b blame_builds; j jobs; answer record;
BEGIN
    PERFORM guard(n,o);
    IF NOT valid_blame_build(n,bk,bt,bg) THEN RETURN QUERY SELECT false,0::bigint; RETURN; END IF;
    SELECT * INTO STRICT b FROM blame_builds WHERE namespace=n AND key_hash=bk;
    -- An existing bound job cannot block a fresh parent after its authority died.
    UPDATE jobs SET lease_until=least(lease_until,clock_timestamp())
        WHERE namespace=n AND key_hash=k AND build_key IS NOT NULL
        AND NOT valid_blame_build(n,build_key,build_token,build_generation);
    SELECT * INTO answer FROM claim(n,o,k,c,t,seconds,hard_seconds);
    IF NOT answer.owned THEN RETURN QUERY SELECT false,answer.attempt::bigint; RETURN; END IF;
    -- Idempotent confirmation must not extend a chunk's immutable hard deadline.
    UPDATE jobs SET build_key=bk,build_token=bt,build_generation=bg,
        hard_until=least(hard_until,b.hard_until),lease_until=least(lease_until,b.hard_until)
        WHERE namespace=n AND key_hash=k RETURNING * INTO j;
    IF NOT valid_job(n,k,t,j.generation) THEN RETURN QUERY SELECT false,j.generation; RETURN; END IF;
    RETURN QUERY SELECT true,j.generation;
END $$;

-- A checkpoint replacement owns a fresh payload attempt, but keeps the old
-- directory readable until the complete new manifest commits. Ordinary claim
-- deliberately still refuses replacing any live ready artifact.
CREATE FUNCTION claim_checkpoint(n uuid,o uuid,k bytea,c bytea,t uuid,seconds integer,hard_seconds integer,
    bt uuid,bg bigint,previous uuid,previous_digest bytea)
RETURNS TABLE(owned boolean,attempt bigint) LANGUAGE plpgsql AS $$
DECLARE j jobs; b blame_builds; old_payload uuid; old_digest bytea;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 60 OR hard_seconds NOT BETWEEN seconds AND 120
        OR (previous IS NULL)<>(previous_digest IS NULL)
        OR (previous_digest IS NOT NULL AND octet_length(previous_digest)<>32) THEN
        RAISE EXCEPTION 'invalid checkpoint claim'; END IF;
    IF NOT valid_blame_build(n,k,bt,bg) THEN RETURN QUERY SELECT false,0::bigint; RETURN; END IF;
    SELECT * INTO STRICT b FROM blame_builds WHERE namespace=n AND key_hash=k;
    IF b.canonical<>c THEN RAISE EXCEPTION 'checkpoint key collision'; END IF;
    SELECT * INTO j FROM jobs WHERE namespace=n AND key_hash=k FOR UPDATE;
    IF FOUND THEN
        IF j.canonical<>c THEN RAISE EXCEPTION 'checkpoint key collision'; END IF;
        IF j.token=t THEN
            RETURN QUERY SELECT j.checkpoint_write AND j.build_key=k AND j.build_token=bt AND j.build_generation=bg
                AND j.checkpoint_previous IS NOT DISTINCT FROM previous AND j.checkpoint_digest IS NOT DISTINCT FROM previous_digest
                AND valid_job(n,k,t,j.generation),j.generation; RETURN;
        END IF;
        IF valid_job(n,k,j.token,j.generation) THEN RETURN QUERY SELECT false,j.generation; RETURN; END IF;
    END IF;
    -- Never convert an immutable body/opaque root into a replaceable checkpoint,
    -- even when that directory has expired. Missing/expired checkpoints may be
    -- replaced with expected=None, but a live one requires its exact version.
    IF EXISTS(SELECT 1 FROM directory d LEFT JOIN manifest_roots m ON m.namespace=d.namespace AND m.payload=d.payload
        WHERE d.namespace=n AND d.key_hash=k AND (m.kind IS NULL OR m.kind<>3)) THEN
        RETURN QUERY SELECT false,0::bigint; RETURN; END IF;
    SELECT d.payload,p.digest INTO old_payload,old_digest FROM directory d JOIN payloads p ON p.namespace=d.namespace AND p.id=d.payload
        WHERE d.namespace=n AND d.key_hash=k AND d.state='ready' AND d.expires_at>clock_timestamp() AND p.state='live';
    IF old_payload IS DISTINCT FROM previous OR old_digest IS DISTINCT FROM previous_digest THEN
        RETURN QUERY SELECT false,0::bigint; RETURN; END IF;
    IF j.namespace IS NULL THEN
        PERFORM charge(n,1,0);
        INSERT INTO jobs(namespace,key_hash,canonical,token,generation,lease_until,hard_until,state)
            VALUES(n,k,c,t,1,least(b.hard_until,clock_timestamp()+make_interval(secs=>seconds)),
                least(b.hard_until,clock_timestamp()+make_interval(secs=>hard_seconds)),'computing') RETURNING * INTO j;
    ELSE
        UPDATE jobs SET token=t,generation=generation+1,state='computing',staged_payload=NULL,
            lease_until=least(b.hard_until,clock_timestamp()+make_interval(secs=>seconds)),
            hard_until=least(b.hard_until,clock_timestamp()+make_interval(secs=>hard_seconds))
            WHERE namespace=n AND key_hash=k RETURNING * INTO j;
    END IF;
    UPDATE jobs SET build_key=k,build_token=bt,build_generation=bg,checkpoint_write=true,
        checkpoint_previous=previous,checkpoint_digest=previous_digest WHERE namespace=n AND key_hash=k;
    IF NOT valid_job(n,k,t,j.generation) THEN RAISE EXCEPTION 'checkpoint parent expired during claim'; END IF;
    RETURN QUERY SELECT true,j.generation;
END $$;

CREATE FUNCTION pin_build_chunk(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,h bytea,s bigint) RETURNS boolean
LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n,o);
    IF NOT valid_blame_build(n,k,t,g) THEN RETURN false; END IF;
    IF NOT EXISTS(SELECT 1 FROM payloads WHERE namespace=n AND id=p AND state='live' AND digest=h AND size=s
        AND size BETWEEN 1 AND 4194304)
        OR EXISTS(SELECT 1 FROM manifest_roots WHERE namespace=n AND payload=p) THEN RETURN false; END IF;
    IF NOT EXISTS(SELECT 1 FROM build_chunks WHERE namespace=n AND build_key=k AND token=t AND generation=g AND child=p) THEN
        IF (SELECT count(*) FROM build_chunks WHERE namespace=n AND build_key=k AND token=t AND generation=g)>=64 THEN
            RAISE EXCEPTION 'blame-build chunk limit exceeded'; END IF;
        PERFORM charge(n,1,0);
        INSERT INTO build_chunks VALUES(n,k,t,g,p);
    END IF;
    RETURN true;
END $$;

CREATE FUNCTION retained_child(n uuid,p uuid) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS(SELECT 1 FROM manifest_children e WHERE e.namespace=n AND e.child=p AND (
        EXISTS(SELECT 1 FROM directory d JOIN payloads root ON root.namespace=d.namespace AND root.id=d.payload
            WHERE d.namespace=n AND d.payload=e.parent AND root.state='live' AND d.state='ready' AND d.expires_at>clock_timestamp())
        OR EXISTS(SELECT 1 FROM read_holds h WHERE h.namespace=n AND h.payload=e.parent AND h.lease_until>clock_timestamp())))
    OR EXISTS(SELECT 1 FROM build_chunks c JOIN blame_builds b ON b.namespace=c.namespace AND b.key_hash=c.build_key
        WHERE c.namespace=n AND c.child=p AND b.token=c.token AND b.generation=c.generation
            AND b.lease_until>clock_timestamp() AND b.hard_until>clock_timestamp());
$$;

CREATE FUNCTION publish_manifest(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer,
    bk bytea,bt uuid,bg bigint,ids text[],hashes bytea[],sizes bigint[],artifact_kind integer) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE total bigint; count_children integer; i integer;
BEGIN
    PERFORM guard(n,o);
    IF NOT valid_blame_build(n,bk,bt,bg) OR k<>bk OR NOT valid_job(n,k,t,g) THEN RETURN false; END IF;
    count_children := cardinality(ids);
    IF count_children IS NULL OR count_children NOT BETWEEN 0 AND 64 OR cardinality(hashes)<>count_children
        OR cardinality(sizes)<>count_children OR artifact_kind NOT BETWEEN 1 AND 3 THEN RAISE EXCEPTION 'invalid artifact manifest'; END IF;
    SELECT coalesce(sum(s),0) INTO total FROM unnest(sizes) AS s;
    IF total>268435456 OR EXISTS(SELECT 1 FROM unnest(sizes) AS s WHERE s IS NULL OR s NOT BETWEEN 1 AND 4194304) THEN
        RAISE EXCEPTION 'invalid artifact sizes'; END IF;
    FOR i IN 1..count_children LOOP
        IF NOT EXISTS(SELECT 1 FROM payloads child JOIN build_chunks c ON c.namespace=child.namespace AND c.child=child.id
            WHERE child.namespace=n AND child.id=ids[i]::uuid AND child.state='live' AND child.digest=hashes[i] AND child.size=sizes[i]
                AND c.build_key=bk AND c.token=bt AND c.generation=bg)
            OR EXISTS(SELECT 1 FROM manifest_roots WHERE namespace=n AND payload=ids[i]::uuid) THEN RETURN false; END IF;
    END LOOP;
    -- publish and edges share one transaction/namespace lock. No reader can see
    -- ready without all verified children, and GC cannot retire a child between.
    IF NOT publish(n,o,k,t,g,p,value,seconds) THEN RETURN false; END IF;
    PERFORM charge(n,count_children+1,0);
    INSERT INTO manifest_roots VALUES(n,p,artifact_kind,count_children,total);
    FOR i IN 1..count_children LOOP
        INSERT INTO manifest_children VALUES(n,p,i-1,ids[i]::uuid,hashes[i],sizes[i]);
    END LOOP;
    -- FK/row-lock waits above must not turn an expired build into a ready root.
    IF NOT valid_blame_build(n,bk,bt,bg) OR NOT EXISTS(SELECT 1 FROM jobs WHERE namespace=n AND key_hash=k
        AND token=t AND generation=g AND hard_until>clock_timestamp() AND lease_until>clock_timestamp()) THEN
        RAISE EXCEPTION 'blame-build expired during manifest publication';
    END IF;
    RETURN true;
END $$;

-- Only this explicit operation replaces a checkpoint. Publication, edges, CAS
-- and release of actual build-pin rows are one namespace-serialized transaction.
-- Old reader holds still retain the old root's children; payloads/intents and
-- their conservative byte/row reservations are never deleted/refunded here.
CREATE FUNCTION publish_checkpoint(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer,
    bk bytea,bt uuid,bg bigint,ids text[],hashes bytea[],sizes bigint[],artifact_kind integer) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE j jobs; old_payload uuid; old_digest bytea; released integer;
BEGIN
    PERFORM guard(n,o);
    IF artifact_kind<>3 OR bk<>k OR NOT valid_blame_build(n,k,bt,bg) OR NOT valid_job(n,k,t,g) THEN RETURN false; END IF;
    SELECT * INTO STRICT j FROM jobs WHERE namespace=n AND key_hash=k;
    IF NOT j.checkpoint_write OR j.build_key IS DISTINCT FROM k OR j.build_token IS DISTINCT FROM bt
        OR j.build_generation IS DISTINCT FROM bg THEN RETURN false; END IF;
    SELECT d.payload,v.digest INTO old_payload,old_digest FROM directory d JOIN payloads v ON v.namespace=d.namespace AND v.id=d.payload
        WHERE d.namespace=n AND d.key_hash=k AND d.state='ready' AND d.expires_at>clock_timestamp() AND v.state='live';
    IF old_payload IS DISTINCT FROM j.checkpoint_previous OR old_digest IS DISTINCT FROM j.checkpoint_digest THEN RETURN false; END IF;
    IF NOT publish_manifest(n,o,k,t,g,p,value,seconds,bk,bt,bg,ids,hashes,sizes,artifact_kind) THEN RETURN false; END IF;
    -- Release only pins whose ownership has actually transferred to this root.
    -- Omitted, still-pending complete units remain owned by the finite build.
    -- This frees LIVE pin slots, not payload/intent/history evidence.
    DELETE FROM build_chunks c WHERE c.namespace=n AND c.build_key=k AND c.token=bt AND c.generation=bg
        AND EXISTS(SELECT 1 FROM manifest_children e WHERE e.namespace=n AND e.parent=p AND e.child=c.child);
    GET DIAGNOSTICS released = ROW_COUNT;
    IF released>0 THEN PERFORM charge(n,-released,0); END IF;
    IF NOT valid_blame_build(n,k,bt,bg) OR NOT EXISTS(SELECT 1 FROM jobs WHERE namespace=n AND key_hash=k AND token=t
        AND generation=g AND lease_until>clock_timestamp() AND hard_until>clock_timestamp())
        OR NOT EXISTS(SELECT 1 FROM directory WHERE namespace=n AND key_hash=k AND payload=p AND state='ready'
            AND expires_at>clock_timestamp()) THEN
        RAISE EXCEPTION 'checkpoint authority or root expired before pin release commit'; END IF;
    RETURN true;
END $$;

CREATE FUNCTION check_manifest(n uuid,o uuid,p uuid,r uuid,artifact_kind integer,count_children integer,total bigint)
RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n,o);
    RETURN EXISTS(SELECT 1 FROM manifest_roots m JOIN read_holds h ON h.namespace=m.namespace AND h.payload=m.payload
        WHERE m.namespace=n AND m.payload=p AND m.kind=artifact_kind AND m.children=count_children AND m.total_bytes=total
            AND h.reader=r AND h.lease_until>clock_timestamp());
END $$;

CREATE FUNCTION acquire_manifest_child(n uuid,o uuid,p uuid,r uuid,child_position integer,reader_id uuid,
    seconds integer,max_size bigint,expected_size bigint,expected_digest bytea)
RETURNS TABLE(payload text,digest bytea,size bigint,object_key text,lifetime_ms bigint) LANGUAGE plpgsql AS $$
DECLARE child payloads; expiry timestamptz;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 120 OR child_position NOT BETWEEN 0 AND 63 OR max_size NOT BETWEEN 1 AND 8388608 THEN
        RAISE EXCEPTION 'invalid manifest read limits'; END IF;
    SELECT h.lease_until INTO expiry FROM read_holds h WHERE h.namespace=n AND h.payload=p AND h.reader=r AND h.lease_until>clock_timestamp();
    IF NOT FOUND THEN RETURN; END IF;
    SELECT v.* INTO child FROM manifest_children e JOIN payloads v ON v.namespace=e.namespace AND v.id=e.child
        WHERE e.namespace=n AND e.parent=p AND e.ordinal=child_position AND e.size=expected_size AND e.digest=expected_digest
            AND v.state='live' AND v.size=e.size AND v.digest=e.digest AND v.size<=max_size;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n,1,0);
    expiry := least(expiry,clock_timestamp()+make_interval(secs=>seconds));
    INSERT INTO read_holds VALUES(n,reader_id,child.id,expiry);
    RETURN QUERY SELECT child.id::text,child.digest,child.size,child.object_key,
        greatest(0,floor(extract(epoch FROM (expiry-clock_timestamp()))*1000)::bigint);
END $$;

CREATE OR REPLACE FUNCTION retire_payloads(n uuid,o uuid,t uuid,g bigint,amount integer,grace integer,intents_only boolean)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE retired bigint;
BEGIN
    PERFORM guard_maintenance(n,o,t,g);
    IF amount NOT BETWEEN 1 AND 100 OR grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid cache retirement limits'; END IF;
    WITH changed AS (UPDATE payloads SET state='retired',retired_at=clock_timestamp(),delete_after=clock_timestamp()+make_interval(secs=>grace)
        WHERE namespace=n AND id IN (
            SELECT p.id FROM payloads p WHERE p.namespace=n AND p.state<>'retired' AND (NOT intents_only OR p.state='staged')
                AND NOT EXISTS(SELECT 1 FROM directory d WHERE d.namespace=n AND d.payload=p.id AND d.state='ready' AND d.expires_at>clock_timestamp())
                AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.namespace=n AND j.staged_payload=p.id AND j.state='computing'
                    AND j.lease_until>clock_timestamp() AND j.hard_until>clock_timestamp())
                AND NOT retained_child(n,p.id)
                ORDER BY p.id LIMIT amount) RETURNING id),
        intents AS (UPDATE upload_intents i SET state='retired',last_result='retired; reconcile late PUT',reconciled_at=clock_timestamp()
            WHERE i.namespace=n AND i.attempt IN (SELECT id FROM changed) AND i.state<>'retired' RETURNING attempt)
    SELECT count(*) INTO retired FROM changed;
    RETURN retired;
END $$;

ALTER TABLE schema_version DROP CONSTRAINT schema_version_version_check;
UPDATE schema_version SET version=2 WHERE singleton;
ALTER TABLE schema_version ADD CONSTRAINT schema_version_version_check CHECK(version=2);
REVOKE ALL ON ALL TABLES IN SCHEMA git_rs_cache FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA git_rs_cache FROM PUBLIC;
COMMIT;
