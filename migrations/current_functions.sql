SET LOCAL search_path = git_rs, pg_catalog;

CREATE OR REPLACE FUNCTION maintenance_enqueue(
    p_repo uuid, p_kind text, p_key text, p_payload bytea DEFAULT ''::bytea,
    p_priority integer DEFAULT 0, p_attempts integer DEFAULT 8
) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE result uuid;
BEGIN
    INSERT INTO maintenance_jobs(id,repo_id,kind,job_key,payload,priority,max_attempts)
        VALUES(gen_random_uuid(),p_repo,p_kind,p_key,p_payload,p_priority,p_attempts)
        ON CONFLICT(job_key) DO NOTHING RETURNING id INTO result;
    IF result IS NULL THEN
        SELECT id INTO STRICT result FROM maintenance_jobs
            WHERE job_key=p_key AND repo_id=p_repo AND kind=p_kind AND payload=p_payload;
    END IF;
    RETURN result;
END $$;

CREATE OR REPLACE FUNCTION maintenance_leadership_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'maintenance leadership identity is permanent'; END IF;
    IF NEW.name IS DISTINCT FROM OLD.name OR NEW.fence<OLD.fence OR NEW.confirmations<OLD.confirmations
       OR (NEW.fence<>OLD.fence AND NEW.fence-OLD.fence<>1)
       OR (NEW.fence=OLD.fence AND NEW.owner_identity IS NOT NULL AND
           (NEW.owner_identity IS DISTINCT FROM OLD.owner_identity OR NEW.campaign_id IS DISTINCT FROM OLD.campaign_id))
    THEN RAISE EXCEPTION 'maintenance leadership fence cannot reset or change owner'; END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintenance_fence_nonreuse ON maintenance_leadership;
CREATE TRIGGER maintenance_fence_nonreuse BEFORE UPDATE OR DELETE ON maintenance_leadership
    FOR EACH ROW EXECUTE FUNCTION maintenance_leadership_identity();

CREATE OR REPLACE FUNCTION maintenance_scan(p_owner text,p_campaign uuid,p_fence bigint,p_batch integer,p_seconds integer,p_budget_ms integer)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record; cursor_id uuid; started timestamptz:=clock_timestamp(); last_id uuid;
BEGIN
    IF p_batch IS NULL OR p_batch NOT BETWEEN 1 AND 1000
       OR p_seconds IS NULL OR p_seconds NOT BETWEEN 1 AND 3600
       OR p_budget_ms IS NULL OR p_budget_ms NOT BETWEEN 1 AND 1000
    THEN RAISE EXCEPTION 'invalid maintenance scanner bounds'; END IF;
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    SELECT repository_cursor INTO cursor_id FROM maintenance_scanner
        WHERE name='source' AND next_scan_at<=clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RETURN; END IF;
    FOR r IN SELECT id,generation FROM repositories WHERE cursor_id IS NULL OR id>cursor_id ORDER BY id LIMIT p_batch LOOP
        IF clock_timestamp()>started+p_budget_ms*interval '1 millisecond' THEN EXIT; END IF;
        IF (SELECT count(*) FROM (SELECT 1 FROM packs WHERE repo_id=r.id LIMIT 8) p)>=8 THEN
            PERFORM maintenance_enqueue(r.id,'compact','compact:'||r.id::text||':'||r.generation::text||':v1',convert_to(r.generation::text,'UTF8'));
        END IF;
        IF EXISTS(SELECT 1 FROM retired_objects WHERE repo_id=r.id AND delete_after<=clock_timestamp())
           AND NOT EXISTS(SELECT 1 FROM maintenance_jobs WHERE repo_id=r.id AND kind='retire_gc' AND state IN ('queued','running','retry'))
           AND NOT EXISTS(SELECT 1 FROM maintenance_jobs WHERE repo_id=r.id AND kind='retire_gc' AND state='dead' AND superseded_by IS NULL) THEN
            PERFORM maintenance_enqueue(r.id,'retire_gc','scan-retire:'||r.id::text||':'||floor(extract(epoch FROM started)/60)::text);
        END IF;
        IF EXISTS(SELECT 1 FROM staged_objects WHERE repo_id=r.id AND retired_at IS NULL AND created_at<clock_timestamp()-interval '60 seconds')
           AND NOT EXISTS(SELECT 1 FROM maintenance_jobs WHERE repo_id=r.id AND kind='orphan_scan' AND state IN ('queued','running','retry'))
           AND NOT EXISTS(SELECT 1 FROM maintenance_jobs WHERE repo_id=r.id AND kind='orphan_scan' AND state='dead' AND superseded_by IS NULL) THEN
            PERFORM maintenance_enqueue(r.id,'orphan_scan','scan-orphan:'||r.id::text||':'||floor(extract(epoch FROM started)/60)::text);
        END IF;
        last_id:=r.id;
    END LOOP;
    IF last_id IS NOT NULL THEN cursor_id:=last_id; END IF;
    IF cursor_id IS NULL OR NOT EXISTS(SELECT 1 FROM repositories WHERE id>cursor_id) THEN cursor_id:=NULL; END IF;
    UPDATE maintenance_scanner SET repository_cursor=cursor_id,
        next_scan_at=clock_timestamp()+make_interval(secs=>p_seconds) WHERE name='source';
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
END $$;

CREATE OR REPLACE FUNCTION maintenance_assert_leader(p_owner text,p_campaign uuid,p_fence bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE l maintenance_leadership%ROWTYPE;
BEGIN
    SELECT * INTO STRICT l FROM maintenance_leadership WHERE name='source' FOR UPDATE;
    IF l.owner_identity IS DISTINCT FROM p_owner OR l.campaign_id IS DISTINCT FROM p_campaign
       OR l.fence IS DISTINCT FROM p_fence OR l.lease_until IS NULL OR l.lease_until<=clock_timestamp()
    THEN RAISE EXCEPTION 'maintenance leadership unavailable'; END IF;
END $$;

CREATE OR REPLACE FUNCTION maintenance_acquire(p_owner text,p_campaign uuid,p_seconds integer)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE l maintenance_leadership%ROWTYPE;
BEGIN
    IF p_owner IS NULL OR octet_length(p_owner) NOT BETWEEN 1 AND 256 OR p_campaign IS NULL
       OR p_seconds IS NULL OR p_seconds NOT BETWEEN 10 AND 120 THEN RAISE EXCEPTION 'invalid leadership request'; END IF;
    SELECT * INTO STRICT l FROM maintenance_leadership WHERE name='source' FOR UPDATE;
    IF l.lease_until>clock_timestamp() THEN
        IF l.owner_identity=p_owner AND l.campaign_id=p_campaign THEN
            UPDATE maintenance_leadership SET confirmations=confirmations+1 WHERE name='source';
            RETURN l.fence;
        END IF;
        RETURN NULL;
    END IF;
    IF l.owner_identity=p_owner AND l.campaign_id=p_campaign THEN
        RAISE EXCEPTION 'expired maintenance campaign cannot revive';
    END IF;
    IF l.fence=9223372036854775807 THEN RAISE EXCEPTION 'maintenance fence exhausted'; END IF;
    UPDATE maintenance_leadership SET fence=fence+1,owner_identity=p_owner,campaign_id=p_campaign,
        lease_until=clock_timestamp()+make_interval(secs=>p_seconds),heartbeat_at=clock_timestamp()
        WHERE name='source' RETURNING fence INTO l.fence;
    RETURN l.fence;
END $$;

CREATE OR REPLACE FUNCTION maintenance_renew(p_owner text,p_campaign uuid,p_fence bigint,p_seconds integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_seconds IS NULL OR p_seconds NOT BETWEEN 10 AND 120 THEN RAISE EXCEPTION 'invalid leadership lease'; END IF;
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    UPDATE maintenance_leadership SET heartbeat_at=clock_timestamp(),
        lease_until=clock_timestamp()+make_interval(secs=>p_seconds) WHERE name='source';
END $$;

CREATE OR REPLACE FUNCTION maintenance_release(p_owner text,p_campaign uuid,p_fence bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    UPDATE maintenance_leadership SET owner_identity=NULL,campaign_id=NULL,lease_until=NULL,
        heartbeat_at=NULL WHERE name='source' AND active_job_id IS NULL;
END $$;

CREATE OR REPLACE FUNCTION maintenance_claim(p_owner text,p_campaign uuid,p_fence bigint,p_token uuid,p_seconds integer)
RETURNS SETOF maintenance_jobs LANGUAGE plpgsql AS $$
DECLARE l maintenance_leadership%ROWTYPE; j maintenance_jobs%ROWTYPE; candidate uuid; repo uuid;
BEGIN
    IF p_token IS NULL OR p_seconds IS NULL OR p_seconds NOT BETWEEN 10 AND 120 THEN RAISE EXCEPTION 'invalid job claim'; END IF;
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    SELECT * INTO STRICT l FROM maintenance_leadership WHERE name='source';
    IF l.active_job_id IS NOT NULL THEN
        SELECT repo_id INTO STRICT repo FROM maintenance_jobs WHERE id=l.active_job_id;
        PERFORM 1 FROM repositories WHERE id=repo FOR UPDATE;
        SELECT * INTO STRICT j FROM maintenance_jobs WHERE id=l.active_job_id FOR UPDATE;
        PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
        IF j.state='running' AND j.lease_until>clock_timestamp() THEN RETURN; END IF;
        IF j.state='running' THEN
            UPDATE maintenance_jobs SET state=CASE
                    WHEN j.kind='compact' AND EXISTS(SELECT 1 FROM maintenance_operations o WHERE o.job_id=j.id
                        AND o.claim_generation=j.claim_generation AND o.leader_fence=j.leader_fence)
                    THEN 'succeeded' WHEN attempts>=max_attempts THEN 'dead' ELSE 'retry' END,
                owner_token=NULL,leader_fence=NULL,lease_until=NULL,heartbeat_at=NULL,
                available_at=clock_timestamp()+interval '5 seconds',updated_at=clock_timestamp(),
                error_code='claim_expired',error_detail='Previous worker did not confirm completion'
                WHERE id=j.id;
        END IF;
        UPDATE maintenance_leadership SET active_job_id=NULL WHERE name='source' AND active_job_id=j.id;
    END IF;
    SELECT id,repo_id INTO candidate,repo FROM maintenance_jobs
        WHERE state IN ('queued','retry') AND available_at<=clock_timestamp()
        ORDER BY priority DESC,available_at,id LIMIT 1;
    IF candidate IS NULL THEN RETURN; END IF;
    PERFORM 1 FROM repositories WHERE id=repo FOR UPDATE;
    SELECT * INTO STRICT j FROM maintenance_jobs WHERE id=candidate FOR UPDATE;
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    IF j.state NOT IN ('queued','retry') OR j.available_at>clock_timestamp() THEN RETURN; END IF;
    IF j.attempts>=j.max_attempts OR j.claim_generation=9223372036854775807 THEN
        UPDATE maintenance_jobs SET state='dead',error_code='attempts_exhausted',updated_at=clock_timestamp() WHERE id=j.id;
        RETURN;
    END IF;
    UPDATE maintenance_jobs SET state='running',attempts=attempts+1,claim_generation=claim_generation+1,
        owner_token=p_token,leader_fence=p_fence,lease_until=clock_timestamp()+make_interval(secs=>p_seconds),
        heartbeat_at=clock_timestamp(),updated_at=clock_timestamp(),error_code=NULL,error_detail=NULL
        WHERE id=j.id RETURNING * INTO j;
    UPDATE maintenance_leadership SET active_job_id=j.id WHERE name='source';
    RETURN NEXT j;
END $$;

CREATE OR REPLACE FUNCTION maintenance_assert_job(p_owner text,p_campaign uuid,p_fence bigint,p_job uuid,p_token uuid,p_generation bigint)
RETURNS maintenance_jobs LANGUAGE plpgsql AS $$
DECLARE j maintenance_jobs%ROWTYPE; repo uuid;
BEGIN
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    SELECT repo_id INTO STRICT repo FROM maintenance_jobs WHERE id=p_job;
    PERFORM 1 FROM repositories WHERE id=repo FOR UPDATE;
    SELECT * INTO STRICT j FROM maintenance_jobs WHERE id=p_job FOR UPDATE;
    -- The repository/job locks may have waited past either absolute DB expiry.
    PERFORM maintenance_assert_leader(p_owner,p_campaign,p_fence);
    IF j.state<>'running' OR j.owner_token IS DISTINCT FROM p_token OR j.claim_generation IS DISTINCT FROM p_generation
       OR j.leader_fence IS DISTINCT FROM p_fence OR j.lease_until IS NULL OR j.lease_until<=clock_timestamp()
       OR NOT EXISTS(SELECT 1 FROM maintenance_leadership WHERE name='source' AND active_job_id=j.id)
    THEN RAISE EXCEPTION 'maintenance job ownership unavailable'; END IF;
    RETURN j;
END $$;

CREATE OR REPLACE FUNCTION maintenance_heartbeat(p_owner text,p_campaign uuid,p_fence bigint,p_job uuid,p_token uuid,p_generation bigint,p_seconds integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_seconds IS NULL OR p_seconds NOT BETWEEN 10 AND 120 THEN RAISE EXCEPTION 'invalid job lease'; END IF;
    PERFORM maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_generation);
    UPDATE maintenance_jobs SET lease_until=clock_timestamp()+make_interval(secs=>p_seconds),
        heartbeat_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=p_job;
END $$;

CREATE OR REPLACE FUNCTION maintenance_finish(p_owner text,p_campaign uuid,p_fence bigint,p_job uuid,p_token uuid,p_generation bigint,
    p_outcome text,p_code text DEFAULT NULL,p_detail text DEFAULT NULL,p_delay integer DEFAULT 5)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE j maintenance_jobs%ROWTYPE; actual text;
BEGIN
    IF p_outcome IS NULL OR p_outcome NOT IN ('succeeded','retry','dead')
       OR p_delay IS NULL OR p_delay NOT BETWEEN 0 AND 300
    THEN RAISE EXCEPTION 'invalid maintenance outcome'; END IF;
    j:=maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_generation);
    actual:=CASE WHEN p_outcome='retry' AND j.attempts>=j.max_attempts THEN 'dead' ELSE p_outcome END;
    INSERT INTO maintenance_job_results(job_id,claim_generation,leader_fence,owner_token,requested_outcome,outcome,error_code,error_detail)
        VALUES(p_job,p_generation,p_fence,p_token,p_outcome,actual,p_code,p_detail);
    PERFORM maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_generation);
    UPDATE maintenance_jobs SET state=actual,
        owner_token=NULL,leader_fence=NULL,lease_until=NULL,heartbeat_at=NULL,
        updated_at=clock_timestamp(),available_at=clock_timestamp()+make_interval(secs=>p_delay),
        error_code=p_code,error_detail=p_detail WHERE id=p_job;
    UPDATE maintenance_leadership SET active_job_id=NULL WHERE name='source' AND active_job_id=p_job;
END $$;

CREATE OR REPLACE FUNCTION maintenance_confirm_finish(p_job uuid,p_token uuid,p_generation bigint,p_fence bigint,
    p_outcome text,p_code text,p_detail text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
    UPDATE maintenance_job_results SET confirmations=confirmations+1
        WHERE job_id=p_job AND owner_token=p_token AND claim_generation=p_generation AND leader_fence=p_fence
            AND requested_outcome=p_outcome AND error_code IS NOT DISTINCT FROM p_code
            AND error_detail IS NOT DISTINCT FROM p_detail RETURNING outcome INTO result;
    RETURN result;
END $$;

-- Qualified catalog writes may come from sessions without the application's
-- startup options. Resolve trigger queries in the source schema, with temporary
-- relations explicitly last so they cannot shadow authoritative source tables.
CREATE OR REPLACE FUNCTION maintenance_protect_key() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, git_rs, pg_temp AS $$
BEGIN
    PERFORM 1 FROM repositories WHERE id=NEW.repo_id FOR UPDATE;
    IF TG_TABLE_NAME='packs' THEN
        IF EXISTS(SELECT 1 FROM retired_objects WHERE object_key IN (NEW.pack_key,NEW.idx_key))
        THEN RAISE EXCEPTION 'retired object key cannot be attached'; END IF;
    ELSIF EXISTS(SELECT 1 FROM retired_objects WHERE object_key=NEW.object_key) THEN
        RAISE EXCEPTION 'retired object key cannot be staged';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintenance_pack_nonreuse ON packs;
CREATE TRIGGER maintenance_pack_nonreuse BEFORE INSERT OR UPDATE ON packs FOR EACH ROW EXECUTE FUNCTION maintenance_protect_key();
DROP TRIGGER IF EXISTS maintenance_stage_nonreuse ON staged_objects;
CREATE TRIGGER maintenance_stage_nonreuse BEFORE INSERT ON staged_objects FOR EACH ROW EXECUTE FUNCTION maintenance_protect_key();

CREATE OR REPLACE FUNCTION maintenance_retirement() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, git_rs, pg_temp AS $$
BEGIN
    IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND
        (NEW.object_key IS DISTINCT FROM OLD.object_key OR NEW.repo_id IS DISTINCT FROM OLD.repo_id
         OR NEW.retired_epoch IS DISTINCT FROM OLD.retired_epoch OR NEW.retirement_operation IS DISTINCT FROM OLD.retirement_operation))
    THEN RAISE EXCEPTION 'retired identity is permanent'; END IF;
    IF TG_OP='INSERT' THEN
        UPDATE staged_objects SET retired_at=NEW.retired_at WHERE object_key=NEW.object_key;
        PERFORM maintenance_enqueue(NEW.repo_id,'retire_gc','retire:'||NEW.repo_id::text||':'||NEW.retirement_operation::text);
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintenance_retired_identity ON retired_objects;
CREATE TRIGGER maintenance_retired_identity BEFORE UPDATE OR DELETE ON retired_objects FOR EACH ROW EXECUTE FUNCTION maintenance_retirement();
DROP TRIGGER IF EXISTS maintenance_retired_enqueue ON retired_objects;
CREATE TRIGGER maintenance_retired_enqueue AFTER INSERT ON retired_objects FOR EACH ROW EXECUTE FUNCTION maintenance_retirement();

CREATE OR REPLACE FUNCTION maintenance_compact_enqueue() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, git_rs, pg_temp AS $$
DECLARE g bigint;
BEGIN
    SELECT generation INTO STRICT g FROM repositories WHERE id=NEW.repo_id;
    IF (SELECT count(*) FROM (SELECT 1 FROM packs WHERE repo_id=NEW.repo_id LIMIT 8) p)>=8 THEN
        PERFORM maintenance_enqueue(NEW.repo_id,'compact','compact:'||NEW.repo_id::text||':'||g::text||':v1',convert_to(g::text,'UTF8'));
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintenance_catalog_enqueue ON packs;
CREATE TRIGGER maintenance_catalog_enqueue AFTER INSERT ON packs FOR EACH ROW EXECUTE FUNCTION maintenance_compact_enqueue();

-- Length-framed, ordered manifests bind physical catalog metadata rather than
-- reachability. sha256(bytea) is the PostgreSQL builtin, not a pgcrypto extension.
CREATE OR REPLACE FUNCTION maintenance_manifest_entry(p_hash text,p_pack text,p_idx text,
    p_pack_digest text,p_idx_digest text,p_pack_size bigint,p_idx_size bigint)
RETURNS bytea LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT int8send(octet_length(convert_to(p_hash,'UTF8'))::bigint)||convert_to(p_hash,'UTF8')
        ||int8send(octet_length(convert_to(p_pack,'UTF8'))::bigint)||convert_to(p_pack,'UTF8')
        ||int8send(octet_length(convert_to(p_idx,'UTF8'))::bigint)||convert_to(p_idx,'UTF8')
        ||int8send(octet_length(convert_to(p_pack_digest,'UTF8'))::bigint)||convert_to(p_pack_digest,'UTF8')
        ||int8send(octet_length(convert_to(p_idx_digest,'UTF8'))::bigint)||convert_to(p_idx_digest,'UTF8')
        ||int8send(p_pack_size)||int8send(p_idx_size)
$$;

CREATE OR REPLACE FUNCTION maintenance_catalog_manifest(p_repo uuid)
RETURNS bytea LANGUAGE plpgsql AS $$
DECLARE n bigint; contents bytea;
BEGIN
    SELECT count(*) INTO n FROM (SELECT 1 FROM packs WHERE repo_id=p_repo LIMIT 4097) p;
    IF n>4096 THEN RAISE EXCEPTION 'maintenance catalog pack bound exceeded'; END IF;
    SELECT coalesce(string_agg(maintenance_manifest_entry(native_hash,pack_key,idx_key,
        pack_digest,idx_digest,pack_size,idx_size),''::bytea ORDER BY native_hash COLLATE "C"),''::bytea)
        INTO contents FROM packs WHERE repo_id=p_repo;
    RETURN sha256(convert_to('git-rs/catalog/v1','UTF8')||decode('00','hex')||int8send(n)||contents);
END $$;

CREATE OR REPLACE FUNCTION maintenance_confirm(p_operation uuid,p_job uuid,p_attempt bigint,p_fence bigint,
    p_generation bigint,p_epoch bigint,p_input bytea,p_output bytea)
RETURNS TABLE(generation bigint,epoch bigint) LANGUAGE plpgsql AS $$
BEGIN
    -- Confirmation is not an execution replay. It cannot attach bytes, change a
    -- catalog or revive expired ownership, and survives later owner truncation.
    RETURN QUERY UPDATE maintenance_operations o SET confirmations=o.confirmations+1
        WHERE o.operation_id=p_operation AND o.job_id=p_job AND o.claim_generation=p_attempt
            AND o.leader_fence=p_fence AND o.input_generation=p_generation AND o.input_epoch=p_epoch
            AND o.input_manifest=p_input AND o.output_manifest=p_output
        RETURNING o.result_generation,o.result_epoch;
END $$;

CREATE OR REPLACE FUNCTION maintenance_journal_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'maintenance operation identity is permanent'; END IF;
    IF (to_jsonb(NEW)-'confirmations') IS DISTINCT FROM (to_jsonb(OLD)-'confirmations')
       OR NEW.confirmations<OLD.confirmations THEN RAISE EXCEPTION 'maintenance operation identity is immutable'; END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS maintenance_journal_nonreuse ON maintenance_operations;
CREATE TRIGGER maintenance_journal_nonreuse BEFORE UPDATE OR DELETE ON maintenance_operations
    FOR EACH ROW EXECUTE FUNCTION maintenance_journal_identity();
DROP TRIGGER IF EXISTS maintenance_results_nonreuse ON maintenance_job_results;
CREATE TRIGGER maintenance_results_nonreuse BEFORE UPDATE OR DELETE ON maintenance_job_results
    FOR EACH ROW EXECUTE FUNCTION maintenance_journal_identity();

CREATE OR REPLACE FUNCTION maintenance_swap(p_owner text,p_campaign uuid,p_fence bigint,p_job uuid,p_token uuid,p_attempt bigint,
    p_barrier uuid,p_generation bigint,p_epoch bigint,p_input bytea,p_output bytea,
    p_hash text,p_pack text,p_idx text,p_pack_digest text,p_idx_digest text,p_pack_size bigint,p_idx_size bigint)
RETURNS TABLE(generation bigint,epoch bigint) LANGUAGE plpgsql AS $$
DECLARE j maintenance_jobs%ROWTYPE; r repositories%ROWTYPE; output bytea;
BEGIN
    j:=maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_attempt);
    IF j.kind<>'compact' OR j.payload_version<>1 THEN RAISE EXCEPTION 'invalid compaction job'; END IF;
    SELECT * INTO STRICT r FROM repositories WHERE id=j.repo_id;
    IF r.generation IS DISTINCT FROM p_generation OR r.storage_epoch IS DISTINCT FROM p_epoch
       OR maintenance_catalog_manifest(r.id) IS DISTINCT FROM p_input THEN RAISE EXCEPTION 'compaction input changed'; END IF;
    IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE id=p_barrier AND repo_id=r.id
        AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'compaction barrier expired'; END IF;
    output:=sha256(convert_to('git-rs/catalog/v1','UTF8')||decode('00','hex')||int8send(1::bigint)
        ||maintenance_manifest_entry(p_hash,p_pack,p_idx,p_pack_digest,p_idx_digest,p_pack_size,p_idx_size));
    IF p_output IS NULL OR output IS DISTINCT FROM p_output
       OR p_pack IS NULL OR p_idx IS NULL OR p_pack=p_idx
       OR p_pack_size IS NULL OR p_idx_size IS NULL OR p_pack_size<=0 OR p_idx_size<=0
       OR (SELECT count(*) FROM staged_objects WHERE repo_id=r.id AND operation_id=p_token
           AND object_key IN(p_pack,p_idx) AND state='uploaded' AND retired_at IS NULL)<>2
       OR EXISTS(SELECT 1 FROM retired_objects WHERE object_key IN(p_pack,p_idx))
    THEN RAISE EXCEPTION 'compaction output intent invalid'; END IF;
    UPDATE repositories SET generation=repositories.generation+1,storage_epoch=storage_epoch+1
        WHERE id=r.id RETURNING * INTO r;
    INSERT INTO retired_objects(repo_id,object_key,retired_epoch,retirement_operation,reason)
        SELECT repo_id,pack_key,r.storage_epoch,p_token,'compaction' FROM packs WHERE repo_id=r.id
        UNION ALL SELECT repo_id,idx_key,r.storage_epoch,p_token,'compaction' FROM packs WHERE repo_id=r.id
        ON CONFLICT(object_key) DO NOTHING;
    DELETE FROM packs WHERE repo_id=r.id;
    INSERT INTO packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation)
        VALUES(r.id,p_token,p_hash,p_pack,p_idx,p_pack_digest,p_idx_digest,p_pack_size,p_idx_size,r.generation);
    INSERT INTO maintenance_operations(operation_id,job_id,claim_generation,leader_fence,input_generation,input_epoch,
        input_manifest,output_manifest,result_generation,result_epoch)
        VALUES(p_token,p_job,p_attempt,p_fence,p_generation,p_epoch,p_input,p_output,r.generation,r.storage_epoch);
    DELETE FROM staged_objects WHERE repo_id=r.id AND operation_id=p_token;
    -- Trigger work or repository locks may have consumed either absolute lease.
    PERFORM maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_attempt);
    IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE id=p_barrier AND repo_id=r.id
        AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'compaction barrier expired'; END IF;
    PERFORM maintenance_finish(p_owner,p_campaign,p_fence,p_job,p_token,p_attempt,'succeeded');
    RETURN QUERY SELECT r.generation,r.storage_epoch;
END $$;

CREATE OR REPLACE FUNCTION maintenance_inspect(p_owner text,p_campaign uuid,p_fence bigint,p_job uuid,p_token uuid,p_attempt bigint,
    p_barrier uuid,p_generation bigint,p_epoch bigint,p_manifest bytea,p_batch integer)
RETURNS TABLE(generation bigint,epoch bigint) LANGUAGE plpgsql AS $$
DECLARE j maintenance_jobs%ROWTYPE; r repositories%ROWTYPE; keys text[];
BEGIN
    IF p_batch IS NULL OR p_batch NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid orphan batch'; END IF;
    j:=maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_attempt);
    IF j.kind<>'orphan_scan' THEN RAISE EXCEPTION 'invalid orphan job'; END IF;
    SELECT * INTO STRICT r FROM repositories WHERE id=j.repo_id;
    IF r.generation IS DISTINCT FROM p_generation OR r.storage_epoch IS DISTINCT FROM p_epoch OR maintenance_catalog_manifest(r.id) IS DISTINCT FROM p_manifest
    THEN RAISE EXCEPTION 'orphan inspection input changed'; END IF;
    IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE id=p_barrier AND repo_id=r.id
        AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'orphan barrier expired'; END IF;
    SELECT array_agg(s.object_key) INTO keys FROM
        (SELECT object_key FROM staged_objects s WHERE s.repo_id=r.id AND s.retired_at IS NULL
          AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.repo_id=r.id AND (p.pack_key=s.object_key OR p.idx_key=s.object_key))
          ORDER BY s.created_at,s.object_key LIMIT p_batch) s;
    IF cardinality(keys)>0 THEN
        UPDATE repositories SET generation=repositories.generation+1,storage_epoch=storage_epoch+1
            WHERE id=r.id RETURNING * INTO r;
        INSERT INTO retired_objects(repo_id,object_key,retired_epoch,retirement_operation,reason)
            SELECT r.id,key,r.storage_epoch,p_token,'orphan-intent' FROM unnest(keys) key
            ON CONFLICT(object_key) DO NOTHING;
    END IF;
    INSERT INTO maintenance_operations(operation_id,job_id,claim_generation,leader_fence,input_generation,input_epoch,
        input_manifest,output_manifest,result_generation,result_epoch)
        VALUES(p_token,p_job,p_attempt,p_fence,p_generation,p_epoch,p_manifest,p_manifest,r.generation,r.storage_epoch);
    PERFORM maintenance_assert_job(p_owner,p_campaign,p_fence,p_job,p_token,p_attempt);
    IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE id=p_barrier AND repo_id=r.id
        AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'orphan barrier expired'; END IF;
    RETURN QUERY SELECT r.generation,r.storage_epoch;
END $$;
