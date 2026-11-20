-- Disposable migrated PostgreSQL only; run as admin with ON_ERROR_STOP=1.
-- Everything, including fixture logins, rolls back. No source/cache S3 needed.
BEGIN;
CREATE ROLE warm_fixture_a LOGIN NOINHERIT;
CREATE ROLE warm_fixture_b LOGIN NOINHERIT;
GRANT USAGE ON SCHEMA git_rs TO warm_fixture_a, warm_fixture_b;
GRANT EXECUTE ON FUNCTION git_rs.warm_slots(), git_rs.warm_acquire(integer,uuid,integer),
    git_rs.warm_identity(uuid), git_rs.warm_snapshot(uuid),
    git_rs.warm_check(uuid,uuid,bigint,bigint,bytea), git_rs.warm_release(uuid)
TO warm_fixture_a, warm_fixture_b;

INSERT INTO git_rs.repositories(id,slug) VALUES ('ac000000-0000-0000-0000-000000000001','warm-example');
INSERT INTO git_rs.permissions(repo_id,principal,role) VALUES ('ac000000-0000-0000-0000-000000000001','warm-reader','reader');
INSERT INTO git_rs.refs VALUES
    ('ac000000-0000-0000-0000-000000000001',convert_to('refs/heads/main','UTF8'),decode(repeat('11',20),'hex')),
    ('ac000000-0000-0000-0000-000000000001',convert_to('refs/heads/unconfigured','UTF8'),decode(repeat('22',20),'hex'));
INSERT INTO git_rs.warm_targets VALUES (
    'warm_fixture_a',1,'ac000000-0000-0000-0000-000000000001','warm-reader',
    convert_to('refs/heads/main','UTF8'),decode('6469722fff','hex'));

-- Both advances happened while the worker was absent. No event or queue row.
UPDATE git_rs.refs SET oid=decode(repeat('33',20),'hex') WHERE name=convert_to('refs/heads/main','UTF8') AND repo_id='ac000000-0000-0000-0000-000000000001';
UPDATE git_rs.refs SET oid=decode(repeat('44',20),'hex') WHERE name=convert_to('refs/heads/main','UTF8') AND repo_id='ac000000-0000-0000-0000-000000000001';
UPDATE git_rs.repositories SET generation=2 WHERE id='ac000000-0000-0000-0000-000000000001';

SET SESSION AUTHORIZATION warm_fixture_a;
DO $$
DECLARE repo CONSTANT uuid := 'ac000000-0000-0000-0000-000000000001';
        hold CONSTANT uuid := 'ac000000-0000-0000-0000-000000000011';
BEGIN
    IF ARRAY(SELECT git_rs.warm_slots()) IS DISTINCT FROM ARRAY[1]
       OR git_rs.warm_acquire(1,hold,90) IS DISTINCT FROM repo THEN
        RAISE EXCEPTION 'did not acquire configured target';
    END IF;
    IF (SELECT count(*) FROM git_rs.warm_snapshot(hold)) <> 2 OR NOT EXISTS (
        SELECT FROM git_rs.warm_snapshot(hold) AS s(line)
        WHERE line = concat_ws(chr(9),'ref','726566732f68656164732f6d61696e',repeat('44',20))
    ) OR NOT EXISTS (
        SELECT FROM git_rs.warm_identity(hold) WHERE principal='warm-reader'
            AND decode(path64,'base64')=decode('6469722fff','hex')
    ) THEN RAISE EXCEPTION 'current branch or raw path mismatch'; END IF;
    IF NOT git_rs.warm_check(hold,repo,2,0,decode(repeat('44',20),'hex'))
       OR git_rs.warm_acquire(1,'ac000000-0000-0000-0000-000000000012',90) IS NOT NULL THEN
        RAISE EXCEPTION 'invalid live hold or duplicate per-target hold';
    END IF;
    IF has_table_privilege('git_rs.refs','INSERT, UPDATE, DELETE')
       OR has_table_privilege('git_rs.warm_targets','SELECT, INSERT, UPDATE, DELETE') THEN
        RAISE EXCEPTION 'worker has unexpected direct access';
    END IF;
END $$;

SET SESSION AUTHORIZATION warm_fixture_b;
DO $$
DECLARE hold CONSTANT uuid := 'ac000000-0000-0000-0000-000000000011';
BEGIN
    IF EXISTS (SELECT FROM git_rs.warm_slots()) OR EXISTS (SELECT FROM git_rs.warm_snapshot(hold)) THEN
        RAISE EXCEPTION 'warm catalog not scoped to login';
    END IF;
    PERFORM git_rs.warm_release(hold);
END $$;

RESET SESSION AUTHORIZATION;
UPDATE git_rs.refs SET oid=decode(repeat('55',20),'hex') WHERE name=convert_to('refs/heads/main','UTF8') AND repo_id='ac000000-0000-0000-0000-000000000001';
SET SESSION AUTHORIZATION warm_fixture_a;
DO $$ BEGIN
    IF git_rs.warm_check('ac000000-0000-0000-0000-000000000011','ac000000-0000-0000-0000-000000000001',2,0,decode(repeat('44',20),'hex')) THEN
        RAISE EXCEPTION 'stale tip was accepted';
    END IF;
    IF NOT git_rs.warm_check('ac000000-0000-0000-0000-000000000011','ac000000-0000-0000-0000-000000000001',2,0,decode(repeat('55',20),'hex')) THEN
        RAISE EXCEPTION 'another login released the hold';
    END IF;
END $$;

RESET SESSION AUTHORIZATION;
UPDATE git_rs.repositories SET storage_epoch=1 WHERE id='ac000000-0000-0000-0000-000000000001';
SET SESSION AUTHORIZATION warm_fixture_a;
DO $$ BEGIN
    IF git_rs.warm_check('ac000000-0000-0000-0000-000000000011','ac000000-0000-0000-0000-000000000001',2,0,decode(repeat('55',20),'hex')) THEN
        RAISE EXCEPTION 'stale epoch was accepted';
    END IF;
END $$;

RESET SESSION AUTHORIZATION;
DELETE FROM git_rs.permissions WHERE repo_id='ac000000-0000-0000-0000-000000000001';
SET SESSION AUTHORIZATION warm_fixture_a;
DO $$
DECLARE hold CONSTANT uuid := 'ac000000-0000-0000-0000-000000000011';
BEGIN
    IF EXISTS (SELECT FROM git_rs.warm_snapshot(hold)) OR
       git_rs.warm_check(hold,'ac000000-0000-0000-0000-000000000001',2,1,decode(repeat('55',20),'hex')) THEN
        RAISE EXCEPTION 'revoked ACL still authorized source';
    END IF;
    PERFORM git_rs.warm_release(hold);
END $$;

RESET SESSION AUTHORIZATION;
DO $$ BEGIN
    IF EXISTS (SELECT FROM git_rs.repository_leases WHERE repo_id='ac000000-0000-0000-0000-000000000001') THEN
        RAISE EXCEPTION 'revoked worker could not release its own hold';
    END IF;
END $$;

-- Optional: same disposable instance with cache schema migrated separately.
-- A contender for the same complete-body key observes Pending; an expired
-- owner is replaced without requiring a push event or manual job repair.
\if :{?warm_cache_fixture}
SET LOCAL search_path=git_rs_cache,pg_catalog;
DO $$
DECLARE n CONSTANT uuid := 'ac000000-0000-0000-0000-000000000021';
        o CONSTANT uuid := 'ac000000-0000-0000-0000-000000000022';
        a CONSTANT uuid := 'ac000000-0000-0000-0000-000000000023';
        b CONSTANT uuid := 'ac000000-0000-0000-0000-000000000024';
        k CONSTANT bytea := decode(repeat('cc',32),'hex');
        c CONSTANT bytea := convert_to('synthetic-complete-body-key','UTF8');
        r record;
BEGIN
    PERFORM git_rs_cache.register_namespace(n,o,100,1048576);
    SELECT * INTO STRICT r FROM git_rs_cache.claim_blame_build(n,o,k,c,a,15,90);
    IF NOT r.owned OR r.attempt<>1 THEN RAISE EXCEPTION 'fixture build was not claimed'; END IF;
    SELECT * INTO STRICT r FROM git_rs_cache.claim_blame_build(n,o,k,c,b,15,90);
    IF r.owned THEN RAISE EXCEPTION 'existing build did not yield Pending'; END IF;
    UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second'
    WHERE namespace=n AND key_hash=k;
    SELECT * INTO STRICT r FROM git_rs_cache.claim_blame_build(n,o,k,c,b,15,90);
    IF NOT r.owned OR r.attempt<>2 THEN RAISE EXCEPTION 'expired build was not recovered'; END IF;
END $$;
\endif
ROLLBACK;
