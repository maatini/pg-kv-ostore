-- =============================================================================
-- V9: Deduplicated Object Storage and Multipart Upload Support
-- =============================================================================

-- Add Status to obj_metadata
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'obj_status') THEN
        CREATE TYPE obj_status AS ENUM ('UPLOADING', 'COMPLETED', 'FAILED');
    END IF;
END
$$;

ALTER TABLE obj_metadata ADD COLUMN IF NOT EXISTS status obj_status DEFAULT 'COMPLETED';

-- Shared chunks table (Content-Addressable Storage)
CREATE TABLE IF NOT EXISTS obj_shared_chunks (
    digest VARCHAR(128) PRIMARY KEY,
    data BYTEA NOT NULL,
    size INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Mapping table between metadata and shared chunks
CREATE TABLE IF NOT EXISTS obj_metadata_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metadata_id UUID NOT NULL REFERENCES obj_metadata(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    chunk_digest VARCHAR(128) NOT NULL REFERENCES obj_shared_chunks(digest),
    UNIQUE (metadata_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_obj_metadata_chunks_metadata ON obj_metadata_chunks(metadata_id);
CREATE INDEX IF NOT EXISTS idx_obj_metadata_chunks_digest ON obj_metadata_chunks(chunk_digest);

-- Note: We keep obj_chunks for backward compatibility or migration if needed,
-- but new logic will use obj_metadata_chunks and obj_shared_chunks.

-- RLS for new tables (if multi-tenancy is active)
-- Since shared chunks are content-addressable, they are technically global,
-- but the mapping (obj_metadata_chunks) is per-metadata, which is already tenant-scoped.
-- If we want to strictly isolate chunks even if they are identical across tenants,
-- we'd need tenant_id in obj_shared_chunks, but that defeats deduplication.
-- Decision: Keep obj_shared_chunks global to maximize deduplication.
-- The access control is enforced at the metadata layer.

ALTER TABLE obj_metadata_chunks ENABLE ROW LEVEL SECURITY;

-- Policy for metadata chunks: access if you can see the metadata
-- (This assumes metadata RLS is already active via TenantContext)
-- Check if V6 exists and applied RLS to metadata
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'obj_metadata' AND rowsecurity = true) THEN
        CREATE POLICY obj_metadata_chunks_tenant_policy ON obj_metadata_chunks
            USING (metadata_id IN (SELECT id FROM obj_metadata));
    END IF;
END
$$;
