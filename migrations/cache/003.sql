-- Access-path-only upgrade: retain all retirement predicates and runtime APIs.
-- Ordinary transactional CREATE INDEX blocks writers while building. Cancellation
-- or failure rolls back both the index and version; retry through cache-migrate.
BEGIN;
SELECT pg_advisory_xact_lock(91079170002);
SET LOCAL search_path = git_rs_cache, pg_catalog;
DO $$ BEGIN
    IF (SELECT version FROM schema_version WHERE singleton) IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION 'cache v3 migration requires v2';
    END IF;
END $$;

-- Reverse lookup for retirement, including old-attempt staged orphans after
-- takeover clears the job pointer. Time-varying lease checks stay in the query,
-- not in a partial-index predicate. Namespace leads the key for source isolation.
CREATE INDEX jobs_staged_payload ON jobs(namespace, staged_payload)
    WHERE state = 'computing' AND staged_payload IS NOT NULL;

-- Keep retained_child's exact predicates visible to the retirement planner.
-- Its volatile SQL-function call otherwise runs before the cheap directory/job
-- exclusions for every protected candidate. Do not mark DB-clock work STABLE or
-- capture one timestamp: authority must still be checked at evaluation time.
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
                AND NOT EXISTS(SELECT 1 FROM manifest_children e WHERE e.namespace=n AND e.child=p.id AND (
                    EXISTS(SELECT 1 FROM directory d JOIN payloads root ON root.namespace=d.namespace AND root.id=d.payload
                        WHERE d.namespace=n AND d.payload=e.parent AND root.state='live' AND d.state='ready' AND d.expires_at>clock_timestamp())
                    OR EXISTS(SELECT 1 FROM read_holds h WHERE h.namespace=n AND h.payload=e.parent AND h.lease_until>clock_timestamp())))
                AND NOT EXISTS(SELECT 1 FROM build_chunks c JOIN blame_builds b ON b.namespace=c.namespace AND b.key_hash=c.build_key
                    WHERE c.namespace=n AND c.child=p.id AND b.token=c.token AND b.generation=c.generation
                        AND b.lease_until>clock_timestamp() AND b.hard_until>clock_timestamp())
                ORDER BY p.id LIMIT amount) RETURNING id),
        intents AS (UPDATE upload_intents i SET state='retired',last_result='retired; reconcile late PUT',reconciled_at=clock_timestamp()
            WHERE i.namespace=n AND i.attempt IN (SELECT id FROM changed) AND i.state<>'retired' RETURNING attempt)
    SELECT count(*) INTO retired FROM changed;
    RETURN retired;
END $$;

ALTER TABLE schema_version DROP CONSTRAINT schema_version_version_check;
UPDATE schema_version SET version=3 WHERE singleton;
ALTER TABLE schema_version ADD CONSTRAINT schema_version_version_check CHECK(version=3);
REVOKE ALL ON ALL TABLES IN SCHEMA git_rs_cache FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA git_rs_cache FROM PUBLIC;
COMMIT;
