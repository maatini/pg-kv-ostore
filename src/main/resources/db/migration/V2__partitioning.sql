-- =============================================================================
-- V2: Partitioning kv_entries for Scalability
-- =============================================================================

-- 1. Rename existing table to back it up (and later copy from it)
ALTER TABLE kv_entries RENAME TO kv_entries_old;

-- Remove the old triggers/indexes from the renamed table to avoid conflicts
DROP TRIGGER IF EXISTS notify_kv_entry_change ON kv_entries_old;
DROP INDEX IF EXISTS idx_kv_entries_bucket_key;
DROP INDEX IF EXISTS idx_kv_entries_bucket_key_revision;
DROP INDEX IF EXISTS idx_kv_entries_created_at;
DROP INDEX IF EXISTS idx_kv_entries_expires_at;
ALTER TABLE kv_entries_old DROP CONSTRAINT IF EXISTS unique_bucket_key_revision;

-- 2. Create the new partitioned table
-- Note: Primary Key MUST include the partition key (bucket_id)
CREATE TABLE kv_entries (
    id UUID DEFAULT uuid_generate_v4(),
    bucket_id UUID NOT NULL REFERENCES kv_buckets(id) ON DELETE CASCADE,
    key VARCHAR(1024) NOT NULL,
    value BYTEA,
    revision BIGINT NOT NULL DEFAULT 1,
    operation VARCHAR(20) NOT NULL DEFAULT 'PUT',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    
    -- New Primary Key structure for partitioning
    PRIMARY KEY (bucket_id, id),
    
    -- Ensure business key uniqueness includes partition key (it already does)
    CONSTRAINT unique_bucket_key_revision UNIQUE (bucket_id, key, revision)
) PARTITION BY HASH (bucket_id);

-- 3. Create Partitions (4 partitions for now)
CREATE TABLE kv_entries_p0 PARTITION OF kv_entries FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE kv_entries_p1 PARTITION OF kv_entries FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE kv_entries_p2 PARTITION OF kv_entries FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE kv_entries_p3 PARTITION OF kv_entries FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- 4. Re-create Indexes
-- Indices are automatically created on partitions
CREATE INDEX idx_kv_entries_bucket_key ON kv_entries(bucket_id, key);
CREATE INDEX idx_kv_entries_bucket_key_revision ON kv_entries(bucket_id, key, revision DESC);
CREATE INDEX idx_kv_entries_created_at ON kv_entries(created_at);
CREATE INDEX idx_kv_entries_expires_at ON kv_entries(expires_at) WHERE expires_at IS NOT NULL;

-- 5. Copy data from old table
-- We assume id is unique enough, but we must use ON CONFLICT just in case of weirdness, 
-- though Insert Select is usually fine.
INSERT INTO kv_entries (id, bucket_id, key, value, revision, operation, created_at, expires_at)
SELECT id, bucket_id, key, value, revision, operation, created_at, expires_at
FROM kv_entries_old;

-- 6. Re-attach Triggers
-- Note: Triggers on partitioned tables are supported in PG 13+
CREATE TRIGGER notify_kv_entry_change
    AFTER INSERT OR UPDATE OR DELETE ON kv_entries
    FOR EACH ROW EXECUTE FUNCTION notify_change();

-- 7. Drop the old table
DROP TABLE kv_entries_old;
