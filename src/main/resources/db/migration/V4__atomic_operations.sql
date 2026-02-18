-- =============================================================================
-- V4: Atomic Stored Procedures for KV Operations
-- =============================================================================

-- Ensure UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Function: get_next_revision
-- Helper to calculate the next revision number for a key
CREATE OR REPLACE FUNCTION get_next_revision(p_bucket_id UUID, p_key VARCHAR) 
RETURNS BIGINT AS $$
DECLARE
    v_last_rev BIGINT;
BEGIN
    SELECT revision INTO v_last_rev
    FROM kv_entries
    WHERE bucket_id = p_bucket_id AND key = p_key
    ORDER BY revision DESC
    LIMIT 1;

    RETURN COALESCE(v_last_rev, 0) + 1;
END;
$$ LANGUAGE plpgsql;

-- Function: kv_put
-- Handles atomic insert of new revision and history cleanup
CREATE OR REPLACE FUNCTION kv_put(
    p_bucket_name VARCHAR,
    p_key VARCHAR,
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
    -- 1. Get Bucket ID and TTL settings
    SELECT id, ttl_seconds, max_history_per_key INTO v_bucket_id, v_bucket_ttl, v_max_history
    FROM kv_buckets WHERE name = p_bucket_name;

    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING HINT = 'Create the bucket first.';
    END IF;

    -- Override max history if passed (optional, logic below)
    -- Actually, if p_max_history is NULL or negative, use bucket default
    IF p_max_history IS NOT NULL AND p_max_history >= 0 THEN
        v_max_history := p_max_history;
    END IF;

    -- 2. Calculate Expiration
    IF p_ttl_seconds IS NOT NULL AND p_ttl_seconds > 0 THEN
        v_expires_at := CURRENT_TIMESTAMP + (CAST(p_ttl_seconds AS TEXT) || ' seconds')::INTERVAL;
    ELSIF v_bucket_ttl IS NOT NULL AND v_bucket_ttl > 0 THEN
        v_expires_at := CURRENT_TIMESTAMP + (v_bucket_ttl || ' seconds')::INTERVAL;
    ELSE
        v_expires_at := NULL; -- No expiration
    END IF;

    -- 3. Get next revision
    v_next_rev := get_next_revision(v_bucket_id, p_key);

    -- 4. Insert new entry
    v_entry_id := uuid_generate_v4();
    INSERT INTO kv_entries (
        id, bucket_id, key, value, revision, operation, created_at, expires_at
    ) VALUES (
        v_entry_id, v_bucket_id, p_key, p_value, v_next_rev, 'PUT', CURRENT_TIMESTAMP, v_expires_at
    ) RETURNING created_at INTO v_created_at;

    -- 5. Cleanup History
    IF v_max_history > 0 THEN
        DELETE FROM kv_entries
        WHERE bucket_id = v_bucket_id AND key = p_key
          AND revision <= v_next_rev - v_max_history;
    END IF;

    -- 6. Notify listeners (if any)
    PERFORM pg_notify(
        'kv_entry_change',
        json_build_object(
            'bucket', p_bucket_name,
            'key', p_key,
            'operation', 'PUT',
            'revision', v_next_rev
        )::text
    );

    -- 7. Return result
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

-- Function: kv_delete
-- Handles atomic logical delete (tombstone)
CREATE OR REPLACE FUNCTION kv_delete(
    p_bucket_name VARCHAR,
    p_key VARCHAR
) RETURNS JSONB AS $$
DECLARE
    v_bucket_id UUID;
    v_next_rev BIGINT;
    v_entry_id UUID;
    v_created_at TIMESTAMP WITH TIME ZONE;
BEGIN
    -- 1. Get Bucket ID
    SELECT id INTO v_bucket_id
    FROM kv_buckets WHERE name = p_bucket_name;

    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING HINT = 'Create the bucket first.';
    END IF;

    -- 1.5. Check if key exists and is not already deleted
    DECLARE
        v_latest_op VARCHAR;
    BEGIN
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
    END;

    -- 2. Get next revision
    v_next_rev := get_next_revision(v_bucket_id, p_key);

    -- 3. Delete existing entries for the key (Logical delete doesn't strictly require deleting old ones unless we prune, 
    -- but user requirements said "Delete existing entries" in some context? 
    -- Actually, usually we keep history. But V3 might have implied strict latest.
    -- Let's stick to insertion of tombstone. 
    -- WAIT: The snippet in Step 1709 had "DELETE FROM kv_entries WHERE bucket_id...".
    -- This means it's not soft delete of history? It's a HARD delete of previous versions?
    -- The user requested "kv_delete (Soft delete with history)" in task.md.
    -- So we should NOT delete old entries unless pruning.
    -- However, the snippet 1709 had explicit DELETE. I will REMOVE it to support history.)
    -- Actuall check Step 1709:
    -- "-- 3. Delete existing entries for the key
    --    DELETE FROM kv_entries WHERE bucket_id = v_bucket_id AND key = p_key;"
    -- This contradicts "Soft delete with history".
    -- I will remove the DELETE command to ensure history is preserved until pruned.
    
    -- 4. Insert Delete Marker
    v_entry_id := uuid_generate_v4();
    INSERT INTO kv_entries (
        id, bucket_id, key, value, revision, operation, created_at
    ) VALUES (
        v_entry_id, v_bucket_id, p_key, NULL, v_next_rev, 'DELETE', CURRENT_TIMESTAMP
    ) RETURNING created_at INTO v_created_at;

    -- 5. Notify listeners
    PERFORM pg_notify(
        'kv_entry_change',
        json_build_object(
            'bucket', p_bucket_name,
            'key', p_key,
            'operation', 'DELETE',
            'revision', v_next_rev
        )::text
    );

    -- 6. Return result
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
