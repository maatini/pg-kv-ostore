-- =============================================================================
-- V4: Atomic Stored Procedures for KV Operations
-- =============================================================================

-- Function: kv_put
-- Handles atomic insert of new revision and history cleanup
CREATE OR REPLACE FUNCTION kv_put(
    p_bucket_name VARCHAR,
    p_key VARCHAR,
    p_value BYTEA,
    p_ttl_seconds BIGINT,
    p_max_history INTEGER
) RETURNS JSONB AS $$
DECLARE
    v_bucket_id UUID;
    v_bucket_ttl BIGINT;
    v_next_rev BIGINT;
    v_expires_at TIMESTAMP WITH TIME ZONE;
    v_entry_id UUID;
    v_created_at TIMESTAMP WITH TIME ZONE;
    v_max_history INTEGER;
BEGIN
    -- 1. Get Bucket ID and TTL settings
    SELECT id, ttl_seconds, max_history_per_key INTO v_bucket_id, v_bucket_ttl, v_max_history
    FROM kv_buckets WHERE name = p_bucket_name;
    
    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING ERRCODE = 'P0002';
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
    END IF;

    -- 3. Get Next Revision (using the sequence table or max+1 strategy)
    -- We use the existing function get_next_revision
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
        WHERE bucket_id = v_bucket_id 
          AND key = p_key 
          AND revision <= (v_next_rev - v_max_history);
    END IF;

    -- 6. Return result as JSON
    RETURN jsonb_build_object(
        'id', v_entry_id,
        'revision', v_next_rev,
        'created_at', v_created_at,
        'bucket_id', v_bucket_id
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
    v_latest_op VARCHAR;
BEGIN
    -- 1. Get Bucket ID
    SELECT id INTO v_bucket_id FROM kv_buckets WHERE name = p_bucket_name;
    
    IF v_bucket_id IS NULL THEN
        RAISE EXCEPTION 'Bucket not found: %', p_bucket_name USING ERRCODE = 'P0002';
    END IF;

    -- 2. Check if key exists and is not already deleted
    -- We check the latest revision
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

    -- 3. Get Next Revision
    v_next_rev := get_next_revision(v_bucket_id, p_key);

    -- 4. Insert Delete Marker
    v_entry_id := uuid_generate_v4();
    INSERT INTO kv_entries (
        id, bucket_id, key, value, revision, operation, created_at
    ) VALUES (
        v_entry_id, v_bucket_id, p_key, NULL, v_next_rev, 'DELETE', CURRENT_TIMESTAMP
    ) RETURNING created_at INTO v_created_at;

    -- 5. Return result
    RETURN jsonb_build_object(
        'id', v_entry_id,
        'revision', v_next_rev,
        'created_at', v_created_at,
        'bucket_id', v_bucket_id
    );
END;
$$ LANGUAGE plpgsql;
