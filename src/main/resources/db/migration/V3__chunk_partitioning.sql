-- =============================================================================
-- V3: Partitioning obj_chunks for Scalability
-- =============================================================================

-- 1. Rename existing table
ALTER TABLE obj_chunks RENAME TO obj_chunks_old;

-- Drop old indexes
DROP INDEX IF EXISTS idx_obj_chunks_metadata_id;
DROP INDEX IF EXISTS idx_obj_chunks_metadata_index;
ALTER TABLE obj_chunks_old DROP CONSTRAINT IF EXISTS unique_metadata_chunk_index;

-- 2. Create partitioned table
-- Partition by HASH of metadata_id to keep chunks of same file together
CREATE TABLE obj_chunks (
    id UUID DEFAULT uuid_generate_v4(),
    metadata_id UUID NOT NULL REFERENCES obj_metadata(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    data BYTEA NOT NULL,
    size INTEGER NOT NULL,
    digest VARCHAR(128),
    
    -- PK MUST include partition key
    PRIMARY KEY (metadata_id, id),
    
    -- Ensure index uniqueness includes partition key
    CONSTRAINT unique_metadata_chunk_index UNIQUE (metadata_id, chunk_index)
) PARTITION BY HASH (metadata_id);

-- 3. Create partitions (4 partitions)
CREATE TABLE obj_chunks_p0 PARTITION OF obj_chunks FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE obj_chunks_p1 PARTITION OF obj_chunks FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE obj_chunks_p2 PARTITION OF obj_chunks FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE obj_chunks_p3 PARTITION OF obj_chunks FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- 4. Re-create Indexes
CREATE INDEX idx_obj_chunks_metadata_index ON obj_chunks(metadata_id, chunk_index);

-- 5. Copy Data
INSERT INTO obj_chunks (id, metadata_id, chunk_index, data, size, digest)
SELECT id, metadata_id, chunk_index, data, size, digest
FROM obj_chunks_old;

-- 6. Drop old table
DROP TABLE obj_chunks_old;
