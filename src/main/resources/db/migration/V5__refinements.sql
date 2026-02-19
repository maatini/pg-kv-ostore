-- =============================================================================
-- V5: Database Refinements & Optimizations
-- =============================================================================

-- 1. Convert key columns to TEXT and add CHECK constraints
-- KV Entries
ALTER TABLE kv_entries 
    ALTER COLUMN key TYPE TEXT,
    ADD CONSTRAINT check_kv_key_length CHECK (length(key) <= 2048);

-- KV Revision Sequences
ALTER TABLE kv_revision_sequences 
    ALTER COLUMN key TYPE TEXT;

-- Object Metadata
ALTER TABLE obj_metadata 
    ALTER COLUMN name TYPE TEXT,
    ADD CONSTRAINT check_obj_name_length CHECK (length(name) <= 2048);

-- Watch Subscriptions
ALTER TABLE watch_subscriptions 
    ALTER COLUMN key_pattern TYPE TEXT;

-- Audit Log
ALTER TABLE audit_log 
    ALTER COLUMN key_or_name TYPE TEXT;

-- 2. Optimize obj_chunks storage
-- Encourage more data in-page or better compression for chunks
-- Note: Must apply to partitions individually
ALTER TABLE obj_chunks_p0 SET (toast_tuple_target = 8160);
ALTER TABLE obj_chunks_p1 SET (toast_tuple_target = 8160);
ALTER TABLE obj_chunks_p2 SET (toast_tuple_target = 8160);
ALTER TABLE obj_chunks_p3 SET (toast_tuple_target = 8160);

-- 3. Increase default chunk size for new buckets (4MB)
ALTER TABLE obj_buckets ALTER COLUMN chunk_size SET DEFAULT 4194304;

-- 4. Add GIN index for potential JSONB values in kv_entries
-- This assumes values might be searched if they contain JSON strings or in future if we use JSONB type.
-- For now, it's a "ready" index if we ever wrap values.
-- Actually, let's wait until we have a specific use case or the user asks for it.
-- One tip was: "GIN-Index auf jsonb-Werte (falls Values als jsonb gespeichert werden)".
-- Our values are currently BYTEA. 
-- Let's stick to what we decided in the plan.

-- 5. Drop old function versions to avoid overloading issues (VARCHAR vs TEXT)
DROP FUNCTION IF EXISTS get_next_revision(UUID, VARCHAR);
DROP FUNCTION IF EXISTS kv_put(VARCHAR, VARCHAR, BYTEA, BIGINT, INTEGER);
DROP FUNCTION IF EXISTS kv_delete(VARCHAR, VARCHAR);

-- 6. Update existing functions to use TEXT for parameters (cleaner)
CREATE OR REPLACE FUNCTION get_next_revision(p_bucket_id UUID, p_key TEXT) 
RETURNS BIGINT AS $$
DECLARE
    v_next_rev BIGINT;
BEGIN
    -- Atomically increment and return the next revision
    INSERT INTO kv_revision_sequences (bucket_id, key, current_revision)
    VALUES (p_bucket_id, p_key, 1)
    ON CONFLICT (bucket_id, key)
    DO UPDATE SET current_revision = kv_revision_sequences.current_revision + 1
    RETURNING current_revision INTO v_next_rev;
    
    RETURN v_next_rev;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION kv_put(
    p_bucket_name VARCHAR,
    p_key TEXT,
    p_value BYTEA,
    p_ttl_seconds BIGINT DEFAULT NULL,
    p_max_history INTEGER DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
    v_bucket_id UUID;
    v_bucket_ttl BIGINT;
    v_max_history INTEGER;
    v_next_rev BIGINT;
    v_expires_at TIMESTAMP WITH TIME ZONE;
    v_entry_id UUID;
    v_created_at TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT id, ttl_seconds, max_history_per_key INTO v_bucket_id, v_bucket_ttl, v_max_history
    FROM kv_buckets WHERE name = p_bucket_name;

    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING HINT = 'Create the bucket first.';
    END IF;

    IF p_max_history IS NOT NULL AND p_max_history >= 0 THEN
        v_max_history := p_max_history;
    END IF;

    IF p_ttl_seconds IS NOT NULL AND p_ttl_seconds > 0 THEN
        v_expires_at := CURRENT_TIMESTAMP + (CAST(p_ttl_seconds AS TEXT) || ' seconds')::INTERVAL;
    ELSIF v_bucket_ttl IS NOT NULL AND v_bucket_ttl > 0 THEN
        v_expires_at := CURRENT_TIMESTAMP + (v_bucket_ttl || ' seconds')::INTERVAL;
    END IF;

    v_next_rev := get_next_revision(v_bucket_id, p_key);
    v_entry_id := uuid_generate_v4();

    INSERT INTO kv_entries (
        id, bucket_id, key, value, revision, operation, created_at, expires_at
    ) VALUES (
        v_entry_id, v_bucket_id, p_key, p_value, v_next_rev, 'PUT', CURRENT_TIMESTAMP, v_expires_at
    ) RETURNING created_at INTO v_created_at;

    IF v_max_history > 0 THEN
        DELETE FROM kv_entries
        WHERE bucket_id = v_bucket_id AND key = p_key
          AND revision <= v_next_rev - v_max_history;
    END IF;

    PERFORM pg_notify(
        'kv_entry_change',
        json_build_object(
            'bucket', p_bucket_name,
            'key', p_key,
            'operation', 'PUT',
            'revision', v_next_rev
        )::text
    );

    RETURN jsonb_build_object(
        'id', v_entry_id,
        'bucket_id', v_bucket_id,
        'bucket', p_bucket_name,
        'key', p_key,
        'value', encode(p_value, 'base64'),
        'revision', v_next_rev,
        'operation', 'PUT',
        'created_at', v_created_at,
        'expires_at', v_expires_at
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION kv_delete(
    p_bucket_name VARCHAR,
    p_key TEXT
) RETURNS JSONB AS $$
DECLARE
    v_bucket_id UUID;
    v_next_rev BIGINT;
    v_entry_id UUID;
    v_created_at TIMESTAMP WITH TIME ZONE;
    v_latest_op VARCHAR;
BEGIN
    SELECT id INTO v_bucket_id FROM kv_buckets WHERE name = p_bucket_name;
    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING HINT = 'Create the bucket first.';
    END IF;

    SELECT operation INTO v_latest_op
    FROM kv_entries
    WHERE bucket_id = v_bucket_id AND key = p_key
    ORDER BY revision DESC LIMIT 1;

    IF v_latest_op IS NULL THEN
        RAISE EXCEPTION 'Key not found: %', p_key USING ERRCODE = 'P0002';
    END IF;

    IF v_latest_op = 'DELETE' THEN
        RAISE EXCEPTION 'Key already deleted: %', p_key USING ERRCODE = 'P0002';
    END IF;

    v_next_rev := get_next_revision(v_bucket_id, p_key);
    v_entry_id := uuid_generate_v4();

    INSERT INTO kv_entries (
        id, bucket_id, key, value, revision, operation, created_at
    ) VALUES (
        v_entry_id, v_bucket_id, p_key, NULL, v_next_rev, 'DELETE', CURRENT_TIMESTAMP
    ) RETURNING created_at INTO v_created_at;

    PERFORM pg_notify(
        'kv_entry_change',
        json_build_object(
            'bucket', p_bucket_name,
            'key', p_key,
            'operation', 'DELETE',
            'revision', v_next_rev
        )::text
    );

    RETURN jsonb_build_object(
        'id', v_entry_id,
        'bucket_id', v_bucket_id,
        'bucket', p_bucket_name,
        'key', p_key,
        'revision', v_next_rev,
        'operation', 'DELETE',
        'created_at', v_created_at
    );
END;
$$ LANGUAGE plpgsql;

-- Function: kv_cas
-- Atomic Compare-And-Swap based on expected revision
CREATE OR REPLACE FUNCTION kv_cas(
    p_bucket_name VARCHAR,
    p_key TEXT,
    p_value BYTEA,
    p_expected_rev BIGINT,
    p_ttl_seconds BIGINT DEFAULT NULL,
    p_max_history INTEGER DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
    v_bucket_id UUID;
    v_actual_rev BIGINT;
BEGIN
    -- 1. Get Bucket ID
    SELECT id INTO v_bucket_id FROM kv_buckets WHERE name = p_bucket_name;
    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING HINT = 'Create the bucket first.';
    END IF;

    -- 2. Lock the key using the revision sequence row
    INSERT INTO kv_revision_sequences (bucket_id, key, current_revision)
    VALUES (v_bucket_id, p_key, 0)
    ON CONFLICT (bucket_id, key)
    DO UPDATE SET current_revision = kv_revision_sequences.current_revision;

    -- 3. Get the actual latest revision from kv_entries (now that we have the lock)
    SELECT revision INTO v_actual_rev
    FROM kv_entries
    WHERE bucket_id = v_bucket_id AND key = p_key
    ORDER BY revision DESC LIMIT 1;

    -- 4. Compare with expected revision
    -- If expected is 0, it means we expect the key NOT to exist
    IF (p_expected_rev = 0 AND v_actual_rev IS NOT NULL) OR 
       (p_expected_rev > 0 AND (v_actual_rev IS NULL OR v_actual_rev != p_expected_rev)) THEN
        RAISE EXCEPTION 'CAS Failure: Expected revision %, but actual revision is %', 
            p_expected_rev, COALESCE(v_actual_rev, 0)
            USING ERRCODE = 'P0003';
    END IF;

    -- 5. If match, perform regular kv_put
    RETURN kv_put(p_bucket_name, p_key, p_value, p_ttl_seconds, p_max_history);
END;
$$ LANGUAGE plpgsql;
