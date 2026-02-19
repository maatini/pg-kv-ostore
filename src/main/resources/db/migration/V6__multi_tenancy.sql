-- =============================================================================
-- V6: Multi-Tenancy with Row Level Security (RLS)
-- =============================================================================

-- 1. Add tenant_id columns
ALTER TABLE kv_entries ADD COLUMN tenant_id VARCHAR(255);
ALTER TABLE obj_metadata ADD COLUMN tenant_id VARCHAR(255);
ALTER TABLE audit_log ADD COLUMN tenant_id VARCHAR(255);

-- 2. Create indices for tenant_id
CREATE INDEX idx_kv_entries_tenant ON kv_entries(tenant_id);
CREATE INDEX idx_obj_metadata_tenant ON obj_metadata(tenant_id);
CREATE INDEX idx_audit_log_tenant ON audit_log(tenant_id);

-- 3. Enable RLS on tables
ALTER TABLE kv_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE obj_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;

-- 4. Define RLS Policies
-- We use a session variable 'app.current_tenant' to identify the current tenant.
-- If the variable is not set (NULL), we deny access (or we could allow admin if needed).

-- KV Entries Policy
CREATE POLICY tenant_kv_isolation ON kv_entries
    USING (tenant_id = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true));

-- Object Metadata Policy
CREATE POLICY tenant_obj_isolation ON obj_metadata
    USING (tenant_id = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true));

-- Audit Log Policy
CREATE POLICY tenant_audit_isolation ON audit_log
    USING (tenant_id = current_setting('app.current_tenant', true))
    WITH CHECK (tenant_id = current_setting('app.current_tenant', true));

-- 5. Add a bypass policy for the 'postgres' superuser or a specific role if needed
-- By default, superusers bypass RLS.

-- 6. Update Functions to be Tenant-Aware
-- We need to ensure get_next_revision and others filter by tenant_id

CREATE OR REPLACE FUNCTION get_next_revision(p_bucket_id UUID, p_key TEXT) 
RETURNS BIGINT AS $$
DECLARE
    v_last_rev BIGINT;
    v_tenant_id TEXT := current_setting('app.current_tenant', true);
BEGIN
    SELECT revision INTO v_last_rev
    FROM kv_entries
    WHERE bucket_id = p_bucket_id AND key = p_key AND tenant_id IS NOT DISTINCT FROM v_tenant_id
    ORDER BY revision DESC
    LIMIT 1;

    RETURN COALESCE(v_last_rev, 0) + 1;
END;
$$ LANGUAGE plpgsql;

-- 7. Trigger to automatically set tenant_id on INSERT if not provided
CREATE OR REPLACE FUNCTION set_tenant_id_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.tenant_id IS NULL THEN
        NEW.tenant_id := current_setting('app.current_tenant', true);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_kv_entries_tenant BEFORE INSERT ON kv_entries
    FOR EACH ROW EXECUTE FUNCTION set_tenant_id_trigger();

CREATE TRIGGER trg_obj_metadata_tenant BEFORE INSERT ON obj_metadata
    FOR EACH ROW EXECUTE FUNCTION set_tenant_id_trigger();

CREATE TRIGGER trg_audit_log_tenant BEFORE INSERT ON audit_log
    FOR EACH ROW EXECUTE FUNCTION set_tenant_id_trigger();
