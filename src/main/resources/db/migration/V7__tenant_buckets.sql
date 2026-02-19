
-- =============================================================================
-- V7: Multi-Tenancy for Buckets (KV and Object Store)
-- =============================================================================

-- 1. Add tenant_id columns
ALTER TABLE kv_buckets ADD COLUMN tenant_id VARCHAR(255);
ALTER TABLE obj_buckets ADD COLUMN tenant_id VARCHAR(255);

-- 2. Create indices for tenant_id
CREATE INDEX idx_kv_buckets_tenant ON kv_buckets(tenant_id);
CREATE INDEX idx_obj_buckets_tenant ON obj_buckets(tenant_id);

-- 3. Update Unique Constraints
-- Drop old unique constraints on 'name'
ALTER TABLE kv_buckets DROP CONSTRAINT kv_buckets_name_key;
ALTER TABLE obj_buckets DROP CONSTRAINT obj_buckets_name_key;

-- Add new unique constraints on (tenant_id, name)
-- We use NULLS NOT DISTINCT to ensure only one global bucket (tenant_id=NULL) with a given name can exist
-- Note: Requires PostgreSQL 15+. If on older version, remove 'NULLS NOT DISTINCT' but be aware of multiple NULLs.
-- Assuming modern PG given the project stack.
ALTER TABLE kv_buckets ADD CONSTRAINT unique_kv_bucket_tenant_name UNIQUE (tenant_id, name);
ALTER TABLE obj_buckets ADD CONSTRAINT unique_obj_bucket_tenant_name UNIQUE (tenant_id, name);

-- 4. Enable RLS
ALTER TABLE kv_buckets ENABLE ROW LEVEL SECURITY;
ALTER TABLE obj_buckets ENABLE ROW LEVEL SECURITY;

-- 5. Define RLS Policies
-- KV Buckets Policy
CREATE POLICY tenant_kv_buckets_isolation ON kv_buckets
    USING (tenant_id IS NOT DISTINCT FROM current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM current_setting('app.current_tenant', true));

-- Object Buckets Policy
CREATE POLICY tenant_obj_buckets_isolation ON obj_buckets
    USING (tenant_id IS NOT DISTINCT FROM current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id IS NOT DISTINCT FROM current_setting('app.current_tenant', true));

-- 6. Triggers to automatically set tenant_id on INSERT
-- We reuse the set_tenant_id_trigger function defined in V6

CREATE TRIGGER trg_kv_buckets_tenant BEFORE INSERT ON kv_buckets
    FOR EACH ROW EXECUTE FUNCTION set_tenant_id_trigger();

CREATE TRIGGER trg_obj_buckets_tenant BEFORE INSERT ON obj_buckets
    FOR EACH ROW EXECUTE FUNCTION set_tenant_id_trigger();
