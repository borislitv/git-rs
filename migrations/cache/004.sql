-- Forward-only physical encoding upgrade. Logical hashes, sizes, quotas and
-- checkpoint CAS are unchanged. Old readers admit raw-v1 only; rollbacks must
-- disable caching, never reinterpret compressed rows or reuse payload identities.
BEGIN;
SELECT pg_advisory_xact_lock(91079170002);
SET LOCAL search_path = git_rs_cache, pg_catalog;
DO $$ BEGIN
    IF (SELECT version FROM schema_version WHERE singleton) IS DISTINCT FROM 3 THEN
        RAISE EXCEPTION 'cache v4 migration requires v3';
    END IF;
END $$;

ALTER TABLE payloads ADD COLUMN encoding text NOT NULL DEFAULT 'raw-v1';
ALTER TABLE payloads ADD COLUMN stored_size bigint;
ALTER TABLE payloads ADD COLUMN stored_digest bytea;
ALTER TABLE payloads ADD CONSTRAINT payloads_physical_encoding_check CHECK (
    (encoding='raw-v1' AND (stored_size IS NULL OR stored_size=size)
        AND (stored_digest IS NULL OR stored_digest=digest))
    OR (encoding='zstd-v1' AND stored_size IS NOT NULL AND stored_digest IS NOT NULL
        AND stored_size BETWEEN 1 AND 8388608 AND octet_length(stored_digest)=32));
-- Locate the frozen v1 anonymous inline-size constraint by its referenced
-- columns, not by PostgreSQL's generated constraint-name numbering.
DO $$
DECLARE constraint_name name;
BEGIN
    SELECT c.conname INTO STRICT constraint_name FROM pg_constraint c
        WHERE c.conrelid='payloads'::regclass AND c.contype='c'
        AND c.conkey @> ARRAY[
            (SELECT attnum FROM pg_attribute WHERE attrelid='payloads'::regclass AND attname='body'),
            (SELECT attnum FROM pg_attribute WHERE attrelid='payloads'::regclass AND attname='size')
        ]::smallint[];
    EXECUTE format('ALTER TABLE payloads DROP CONSTRAINT %I',constraint_name);
END $$;
ALTER TABLE payloads ADD CONSTRAINT payloads_stored_body_size_check CHECK (
    body IS NULL OR (object_key IS NULL AND octet_length(body)=coalesce(stored_size,size)));

CREATE FUNCTION prepare_payload_v4(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,h bytea,s bigint,
    external boolean,codec text,physical_hash bytea,physical_size bigint) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE previous uuid;
BEGIN
    PERFORM guard(n,o);
    IF codec IS NULL OR codec NOT IN ('raw-v1','zstd-v1') OR h IS NULL OR octet_length(h)<>32
        OR s IS NULL OR s NOT BETWEEN 0 AND 8388608 OR physical_hash IS NULL OR octet_length(physical_hash)<>32
        OR physical_size IS NULL OR physical_size NOT BETWEEN 0 AND 8388608 OR external IS NULL
        OR (codec='raw-v1' AND (physical_size<>s OR physical_hash<>h))
        OR (codec='zstd-v1' AND physical_size=0) THEN RAISE EXCEPTION 'invalid cache encoding metadata'; END IF;
    IF NOT valid_job(n,k,t,g) THEN RETURN false; END IF;
    SELECT staged_payload INTO previous FROM jobs WHERE namespace=n AND key_hash=k;
    IF previous IS NOT NULL THEN
        RETURN previous=p AND EXISTS(SELECT 1 FROM payloads WHERE namespace=n AND id=p
            AND key_hash=k AND token=t AND generation=g AND digest=h AND size=s AND encoding=codec
            AND coalesce(stored_digest,digest)=physical_hash AND coalesce(stored_size,size)=physical_size
            AND (object_key IS NOT NULL)=external AND state='staged');
    END IF;
    -- Keep the logical reservation even for compressed S3 data and tombstones.
    PERFORM charge(n,CASE WHEN external THEN 2 ELSE 1 END,s);
    INSERT INTO payloads(namespace,id,key_hash,token,generation,object_key,digest,size,state,
        encoding,stored_digest,stored_size) VALUES(n,p,k,t,g,
        CASE WHEN external THEN 'cache/' || n::text || '/' || p::text END,h,s,'staged',codec,
        CASE WHEN codec='zstd-v1' THEN physical_hash END,CASE WHEN codec='zstd-v1' THEN physical_size END);
    IF external THEN INSERT INTO upload_intents(namespace,attempt,state) VALUES(n,p,'planned'); END IF;
    UPDATE jobs SET staged_payload=p WHERE namespace=n AND key_hash=k;
    RETURN true;
END $$;

-- A raw retry cannot confirm, overwrite or reinterpret a compressed attempt.
CREATE OR REPLACE FUNCTION prepare_payload(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,h bytea,s bigint,external boolean)
RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN
    RETURN prepare_payload_v4(n,o,k,t,g,p,h,s,external,'raw-v1',h,s);
END $$;

CREATE FUNCTION acquire_read_v4(n uuid,o uuid,k bytea,c bytea,r uuid,seconds integer,max_size bigint)
RETURNS TABLE(payload text,digest bytea,size bigint,object_key text,lifetime_ms bigint,
    encoding text,stored_size bigint,stored_digest bytea) LANGUAGE plpgsql AS $$
DECLARE v payloads; expiry timestamptz;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 120 OR max_size NOT BETWEEN 1 AND 8388608 THEN RAISE EXCEPTION 'invalid cache read limits'; END IF;
    SELECT p.* INTO v FROM directory d JOIN jobs j USING(namespace,key_hash)
        JOIN payloads p ON p.namespace=d.namespace AND p.id=d.payload
        WHERE d.namespace=n AND d.key_hash=k AND j.canonical=c AND d.state='ready'
        AND d.expires_at>clock_timestamp() AND p.state='live' AND p.size<=max_size
        AND coalesce(p.stored_size,p.size)<=max_size;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n,1,0);
    expiry := clock_timestamp()+make_interval(secs=>seconds);
    INSERT INTO read_holds VALUES(n,r,v.id,expiry);
    RETURN QUERY SELECT v.id::text,v.digest,v.size,v.object_key,
        greatest(0,floor(extract(epoch FROM (expiry-clock_timestamp()))*1000)::bigint),
        v.encoding,coalesce(v.stored_size,v.size),coalesce(v.stored_digest,v.digest);
END $$;

CREATE OR REPLACE FUNCTION acquire_read(n uuid,o uuid,k bytea,c bytea,r uuid,seconds integer,max_size bigint)
RETURNS TABLE(payload text,digest bytea,size bigint,object_key text,lifetime_ms bigint) LANGUAGE plpgsql AS $$
DECLARE v payloads; expiry timestamptz;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 120 OR max_size NOT BETWEEN 1 AND 8388608 THEN RAISE EXCEPTION 'invalid cache read limits'; END IF;
    SELECT p.* INTO v FROM directory d JOIN jobs j USING(namespace,key_hash)
        JOIN payloads p ON p.namespace=d.namespace AND p.id=d.payload
        WHERE d.namespace=n AND d.key_hash=k AND j.canonical=c AND d.state='ready'
        AND d.expires_at>clock_timestamp() AND p.state='live' AND p.size<=max_size AND p.encoding='raw-v1';
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n,1,0);
    expiry := clock_timestamp()+make_interval(secs=>seconds);
    INSERT INTO read_holds VALUES(n,r,v.id,expiry);
    RETURN QUERY SELECT v.id::text,v.digest,v.size,v.object_key,
        greatest(0,floor(extract(epoch FROM (expiry-clock_timestamp()))*1000)::bigint);
END $$;

CREATE FUNCTION acquire_manifest_child_v4(n uuid,o uuid,p uuid,r uuid,child_position integer,reader_id uuid,
    seconds integer,max_size bigint,expected_size bigint,expected_digest bytea)
RETURNS TABLE(payload text,digest bytea,size bigint,object_key text,lifetime_ms bigint,
    encoding text,stored_size bigint,stored_digest bytea) LANGUAGE plpgsql AS $$
DECLARE child payloads; expiry timestamptz;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 120 OR child_position NOT BETWEEN 0 AND 63 OR max_size NOT BETWEEN 1 AND 8388608 THEN
        RAISE EXCEPTION 'invalid manifest read limits'; END IF;
    SELECT h.lease_until INTO expiry FROM read_holds h WHERE h.namespace=n AND h.payload=p AND h.reader=r AND h.lease_until>clock_timestamp();
    IF NOT FOUND THEN RETURN; END IF;
    SELECT v.* INTO child FROM manifest_children e JOIN payloads v ON v.namespace=e.namespace AND v.id=e.child
        WHERE e.namespace=n AND e.parent=p AND e.ordinal=child_position AND e.size=expected_size AND e.digest=expected_digest
            AND v.state='live' AND v.size=e.size AND v.digest=e.digest AND v.size<=max_size
            AND coalesce(v.stored_size,v.size)<=max_size;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n,1,0);
    expiry := least(expiry,clock_timestamp()+make_interval(secs=>seconds));
    INSERT INTO read_holds VALUES(n,reader_id,child.id,expiry);
    RETURN QUERY SELECT child.id::text,child.digest,child.size,child.object_key,
        greatest(0,floor(extract(epoch FROM (expiry-clock_timestamp()))*1000)::bigint),
        child.encoding,coalesce(child.stored_size,child.size),coalesce(child.stored_digest,child.digest);
END $$;

CREATE OR REPLACE FUNCTION acquire_manifest_child(n uuid,o uuid,p uuid,r uuid,child_position integer,reader_id uuid,
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
            AND v.state='live' AND v.size=e.size AND v.digest=e.digest AND v.size<=max_size AND v.encoding='raw-v1';
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM charge(n,1,0);
    expiry := least(expiry,clock_timestamp()+make_interval(secs=>seconds));
    INSERT INTO read_holds VALUES(n,reader_id,child.id,expiry);
    RETURN QUERY SELECT child.id::text,child.digest,child.size,child.object_key,
        greatest(0,floor(extract(epoch FROM (expiry-clock_timestamp()))*1000)::bigint);
END $$;

CREATE FUNCTION publish_v4(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE v payloads;
BEGIN
    PERFORM guard(n,o);
    IF seconds NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid cache TTL'; END IF;
    IF NOT valid_job(n,k,t,g) THEN RETURN false; END IF;
    SELECT * INTO v FROM payloads WHERE namespace=n AND id=p FOR UPDATE;
    IF NOT FOUND OR v.state<>'staged' OR v.key_hash<>k OR v.token<>t OR v.generation<>g THEN RETURN false; END IF;
    PERFORM 1 FROM directory WHERE namespace=n AND key_hash=k FOR UPDATE;
    IF NOT valid_job(n,k,t,g) THEN RETURN false; END IF;
    IF v.object_key IS NULL THEN
        IF value IS NULL OR octet_length(value)<>coalesce(v.stored_size,v.size) THEN RAISE EXCEPTION 'invalid inline cache payload'; END IF;
    ELSIF value IS NOT NULL OR NOT EXISTS(SELECT 1 FROM upload_intents WHERE namespace=n AND attempt=p AND state='confirmed') THEN
        RETURN false;
    END IF;
    IF NOT EXISTS(SELECT 1 FROM directory WHERE namespace=n AND key_hash=k) THEN PERFORM charge(n,1,0); END IF;
    UPDATE payloads SET state='live',body=value WHERE namespace=n AND id=p;
    INSERT INTO directory(namespace,key_hash,payload,state,expires_at)
        VALUES(n,k,p,'ready',clock_timestamp()+make_interval(secs=>seconds))
        ON CONFLICT(namespace,key_hash) DO UPDATE SET payload=excluded.payload,state='ready',expires_at=excluded.expires_at;
    UPDATE jobs SET state='ready' WHERE namespace=n AND key_hash=k;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION publish(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer)
RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN
    PERFORM guard(n,o);
    IF NOT EXISTS(SELECT 1 FROM payloads WHERE namespace=n AND id=p AND encoding='raw-v1') THEN RETURN false; END IF;
    RETURN publish_v4(n,o,k,t,g,p,value,seconds);
END $$;

CREATE FUNCTION publish_manifest_v4(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer,
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
    IF NOT publish_v4(n,o,k,t,g,p,value,seconds) THEN RETURN false; END IF;
    PERFORM charge(n,count_children+1,0);
    INSERT INTO manifest_roots VALUES(n,p,artifact_kind,count_children,total);
    FOR i IN 1..count_children LOOP
        INSERT INTO manifest_children VALUES(n,p,i-1,ids[i]::uuid,hashes[i],sizes[i]);
    END LOOP;
    IF NOT valid_blame_build(n,bk,bt,bg) OR NOT EXISTS(SELECT 1 FROM jobs WHERE namespace=n AND key_hash=k
        AND token=t AND generation=g AND hard_until>clock_timestamp() AND lease_until>clock_timestamp()) THEN
        RAISE EXCEPTION 'blame-build expired during manifest publication';
    END IF;
    RETURN true;
END $$;

CREATE FUNCTION publish_checkpoint_v4(n uuid,o uuid,k bytea,t uuid,g bigint,p uuid,value bytea,seconds integer,
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
    IF NOT publish_manifest_v4(n,o,k,t,g,p,value,seconds,bk,bt,bg,ids,hashes,sizes,artifact_kind) THEN RETURN false; END IF;
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

ALTER TABLE schema_version DROP CONSTRAINT schema_version_version_check;
UPDATE schema_version SET version=4 WHERE singleton;
ALTER TABLE schema_version ADD CONSTRAINT schema_version_version_check CHECK(version=4);
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA git_rs_cache FROM PUBLIC;
COMMIT;
