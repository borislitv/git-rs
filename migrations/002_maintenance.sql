-- Applied only after the serialized initializer has validated the singleton.
SET LOCAL search_path = git_rs, pg_catalog;
DO $upgrade$ BEGIN
IF (SELECT version FROM schema_version) = 1 THEN
ALTER TABLE schema_version DROP CONSTRAINT schema_version_version_check;
ALTER TABLE schema_version ADD CONSTRAINT schema_version_version_check CHECK (version IN (1,2));

CREATE TABLE maintenance_leadership (
    name text PRIMARY KEY CHECK (name = 'source'),
    fence bigint NOT NULL DEFAULT 0 CHECK (fence >= 0),
    owner_identity text CHECK (octet_length(owner_identity) BETWEEN 1 AND 256),
    campaign_id uuid,
    lease_until timestamptz,
    heartbeat_at timestamptz,
    active_job_id uuid,
    confirmations bigint NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    CHECK ((owner_identity IS NULL) = (campaign_id IS NULL)),
    CHECK ((owner_identity IS NULL) = (lease_until IS NULL))
);
INSERT INTO maintenance_leadership(name) VALUES ('source');

CREATE TABLE maintenance_jobs (
    id uuid PRIMARY KEY,
    repo_id uuid NOT NULL REFERENCES repositories(id),
    kind text NOT NULL CHECK (kind IN ('compact','full_gc','retire_gc','orphan_scan')),
    payload_version integer NOT NULL DEFAULT 1 CHECK (payload_version = 1),
    payload bytea NOT NULL DEFAULT ''::bytea CHECK (octet_length(payload) <= 2048),
    job_key text NOT NULL UNIQUE CHECK (octet_length(job_key) BETWEEN 1 AND 256),
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','retry','succeeded','dead')),
    priority integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 16),
    owner_token uuid,
    claim_generation bigint NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    leader_fence bigint CHECK (leader_fence >= 0),
    lease_until timestamptz,
    heartbeat_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    error_code text CHECK (octet_length(error_code) <= 64),
    error_detail text CHECK (octet_length(error_detail) <= 1024),
    superseded_by uuid REFERENCES maintenance_jobs(id),
    CHECK (attempts <= max_attempts),
    CHECK ((state = 'running') = (owner_token IS NOT NULL)),
    CHECK ((state = 'running') = (leader_fence IS NOT NULL)),
    CHECK ((state = 'running') = (lease_until IS NOT NULL)),
    CHECK ((state = 'running') = (heartbeat_at IS NOT NULL))
);
CREATE INDEX maintenance_runnable ON maintenance_jobs(priority DESC,available_at,id) WHERE state IN ('queued','retry');
CREATE INDEX maintenance_expired ON maintenance_jobs(lease_until,id) WHERE state='running';
CREATE INDEX maintenance_reconcile ON maintenance_jobs(repo_id,kind,state);
CREATE INDEX maintenance_dead_blockers ON maintenance_jobs(repo_id,kind) WHERE state='dead' AND superseded_by IS NULL;
ALTER TABLE maintenance_leadership ADD CONSTRAINT maintenance_active_job
    FOREIGN KEY (active_job_id) REFERENCES maintenance_jobs(id);

CREATE TABLE maintenance_operations (
    operation_id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES maintenance_jobs(id),
    claim_generation bigint NOT NULL CHECK (claim_generation > 0),
    leader_fence bigint NOT NULL CHECK (leader_fence > 0),
    input_generation bigint NOT NULL CHECK (input_generation >= 0),
    input_epoch bigint NOT NULL CHECK (input_epoch >= 0),
    input_manifest bytea NOT NULL CHECK (octet_length(input_manifest) = 32),
    output_manifest bytea NOT NULL CHECK (octet_length(output_manifest) = 32),
    result_generation bigint NOT NULL CHECK (result_generation >= 0),
    result_epoch bigint NOT NULL CHECK (result_epoch >= 0),
    confirmations bigint NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,claim_generation,leader_fence)
);

-- Terminal/retry acknowledgments also need exact unknown-COMMIT recovery. One
-- bounded row per claimed attempt preserves it after the job is claimed again.
CREATE TABLE maintenance_job_results (
    job_id uuid NOT NULL REFERENCES maintenance_jobs(id),
    claim_generation bigint NOT NULL CHECK (claim_generation>0),
    leader_fence bigint NOT NULL CHECK (leader_fence>0),
    owner_token uuid NOT NULL,
    requested_outcome text NOT NULL CHECK (requested_outcome IN ('succeeded','retry','dead')),
    outcome text NOT NULL CHECK (outcome IN ('succeeded','retry','dead')),
    error_code text CHECK (octet_length(error_code)<=64),
    error_detail text CHECK (octet_length(error_detail)<=1024),
    confirmations bigint NOT NULL DEFAULT 0 CHECK (confirmations>=0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(job_id,claim_generation,leader_fence)
);

ALTER TABLE retired_objects
    ADD COLUMN retirement_operation uuid NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN reason text NOT NULL DEFAULT 'legacy' CHECK (octet_length(reason) BETWEEN 1 AND 64),
    ADD COLUMN retired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN delete_after timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN last_delete_at timestamptz,
    ADD COLUMN delete_attempts bigint NOT NULL DEFAULT 0 CHECK (delete_attempts >= 0);
CREATE INDEX retired_delete_scan ON retired_objects(repo_id,delete_after,object_key);
-- Retain planned evidence permanently after retirement, without repeatedly
-- walking those historical rows to find the next uninvestigated intent.
ALTER TABLE staged_objects ADD COLUMN retired_at timestamptz;
UPDATE staged_objects s SET retired_at=t.retired_at FROM retired_objects t WHERE t.object_key=s.object_key;
CREATE INDEX staged_inspection_scan ON staged_objects(repo_id,created_at,object_key) WHERE retired_at IS NULL;

CREATE TABLE maintenance_upload_scans (
    repo_id uuid PRIMARY KEY REFERENCES repositories(id),
    key_marker text NOT NULL DEFAULT '' CHECK (octet_length(key_marker)<=1024),
    upload_marker text NOT NULL DEFAULT '' CHECK (octet_length(upload_marker)<=1024)
);

CREATE TABLE maintenance_scanner (
    name text PRIMARY KEY CHECK (name = 'source'),
    repository_cursor uuid,
    next_scan_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO maintenance_scanner(name) VALUES ('source');

UPDATE schema_version SET version=2;
END IF;
END $upgrade$;
