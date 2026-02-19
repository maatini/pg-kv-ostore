
-- =============================================================================
-- V8: Fix RLS NULL Handling and FORCE RLS for Owner
-- =============================================================================

-- Enable FORCE RLS on all tables to ensure isolation even for the owner (postgres)
ALTER TABLE kv_entries FORCE ROW LEVEL SECURITY;
ALTER TABLE obj_metadata FORCE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;
ALTER TABLE kv_buckets FORCE ROW LEVEL SECURITY;
ALTER TABLE obj_buckets FORCE ROW LEVEL SECURITY;
ALTER TABLE obj_chunks FORCE ROW LEVEL SECURITY;

-- Update policies to use IS NOT DISTINCT FROM instead of =
-- This ensures that global entries (tenant_id IS NULL) are accessible 
-- when current_setting('app.current_tenant') is NULL.

-- 1. KV Entries
DROP POLICY IF EXISTS tenant_kv_isolation ON kv_entries;
CREATE POLICY tenant_kv_isolation ON kv_entries
    USING (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''));

-- 2. Object Metadata
DROP POLICY IF EXISTS tenant_obj_isolation ON obj_metadata;
CREATE POLICY tenant_obj_isolation ON obj_metadata
    USING (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''));

-- 3. Audit Log
DROP POLICY IF EXISTS tenant_audit_isolation ON audit_log;
CREATE POLICY tenant_audit_isolation ON audit_log
    USING (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''));

-- 4. Update get_next_revision to also handle empty string as NULL
CREATE OR REPLACE FUNCTION get_next_revision(p_bucket_id UUID, p_key TEXT) 
RETURNS BIGINT AS $$
DECLARE
    v_last_rev BIGINT;
    v_tenant_id TEXT := NULLIF(current_setting('app.current_tenant', true), '');
BEGIN
    SELECT revision INTO v_last_rev
    FROM kv_entries
    WHERE bucket_id = p_bucket_id AND key = p_key 
      AND tenant_id IS NOT DISTINCT FROM v_tenant_id
    ORDER BY revision DESC
    LIMIT 1;

    RETURN COALESCE(v_last_rev, 0) + 1;
END;
$$ LANGUAGE plpgsql;

-- 5. Update Bucket Policies to use IS NOT DISTINCT FROM and NULLIF
DROP POLICY IF EXISTS tenant_kv_buckets_isolation ON kv_buckets;
CREATE POLICY tenant_kv_buckets_isolation ON kv_buckets
    USING (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''));

DROP POLICY IF EXISTS tenant_obj_buckets_isolation ON obj_buckets;
CREATE POLICY tenant_obj_buckets_isolation ON obj_buckets
    USING (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM NULLIF(current_setting('app.current_tenant', true), ''));

-- 6. Enable RLS on obj_chunks (partitioned table)
-- Note: In PG, RLS on partitioned table applies to all partitions.
-- But we need a tenant_id on chunks too if we want to isolate them at that level.
-- However, chunks are currently isolated by the fact that they are only reachable
-- via obj_metadata which HAS RLS. 
-- For defense in depth, we could add tenant_id to chunks, but that's a larger change.
-- For now, fixing the existing policies is the priority for the failing tests.
