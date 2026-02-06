-- =============================================================================
-- V1: Initial Schema for KV and Object Store
-- =============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Key-Value Store Tables
-- =============================================================================

-- KV Buckets table
CREATE TABLE kv_buckets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    max_value_size INTEGER DEFAULT 1048576,
    max_history_per_key INTEGER DEFAULT 100,
    ttl_seconds BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_kv_buckets_name ON kv_buckets(name);

-- KV Entries table (stores current and historical values)
CREATE TABLE kv_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bucket_id UUID NOT NULL REFERENCES kv_buckets(id) ON DELETE CASCADE,
    key VARCHAR(1024) NOT NULL,
    value BYTEA,
    revision BIGINT NOT NULL DEFAULT 1,
    operation VARCHAR(20) NOT NULL DEFAULT 'PUT',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT unique_bucket_key_revision UNIQUE (bucket_id, key, revision)
);

CREATE INDEX idx_kv_entries_bucket_id ON kv_entries(bucket_id);
CREATE INDEX idx_kv_entries_bucket_key ON kv_entries(bucket_id, key);
CREATE INDEX idx_kv_entries_bucket_key_revision ON kv_entries(bucket_id, key, revision DESC);
CREATE INDEX idx_kv_entries_created_at ON kv_entries(created_at);
CREATE INDEX idx_kv_entries_expires_at ON kv_entries(expires_at) WHERE expires_at IS NOT NULL;

-- Sequence for revision numbers per bucket/key
CREATE TABLE kv_revision_sequences (
    bucket_id UUID NOT NULL REFERENCES kv_buckets(id) ON DELETE CASCADE,
    key VARCHAR(1024) NOT NULL,
    current_revision BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, key)
);

-- =============================================================================
-- Object Store Tables
-- =============================================================================

-- Object Buckets table
CREATE TABLE obj_buckets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    chunk_size INTEGER DEFAULT 1048576,
    max_object_size BIGINT DEFAULT 1073741824,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_obj_buckets_name ON obj_buckets(name);

-- Object Metadata table
CREATE TABLE obj_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bucket_id UUID NOT NULL REFERENCES obj_buckets(id) ON DELETE CASCADE,
    name VARCHAR(1024) NOT NULL,
    size BIGINT NOT NULL,
    chunk_count INTEGER NOT NULL,
    digest VARCHAR(128),
    digest_algorithm VARCHAR(32) DEFAULT 'SHA-256',
    content_type VARCHAR(255),
    description TEXT,
    headers JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_bucket_object_name UNIQUE (bucket_id, name)
);

CREATE INDEX idx_obj_metadata_bucket_id ON obj_metadata(bucket_id);
CREATE INDEX idx_obj_metadata_bucket_name ON obj_metadata(bucket_id, name);
CREATE INDEX idx_obj_metadata_created_at ON obj_metadata(created_at);

-- Object Chunks table
CREATE TABLE obj_chunks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metadata_id UUID NOT NULL REFERENCES obj_metadata(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    data BYTEA NOT NULL,
    size INTEGER NOT NULL,
    digest VARCHAR(128),
    CONSTRAINT unique_metadata_chunk_index UNIQUE (metadata_id, chunk_index)
);

CREATE INDEX idx_obj_chunks_metadata_id ON obj_chunks(metadata_id);
CREATE INDEX idx_obj_chunks_metadata_index ON obj_chunks(metadata_id, chunk_index);

-- =============================================================================
-- Watch/Notification Support Tables
-- =============================================================================

-- Watch subscriptions table
CREATE TABLE watch_subscriptions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_type VARCHAR(20) NOT NULL, -- 'KV' or 'OBJECT'
    bucket_id UUID NOT NULL,
    key_pattern VARCHAR(1024),
    session_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_watch_subscriptions_store_bucket ON watch_subscriptions(store_type, bucket_id);
CREATE INDEX idx_watch_subscriptions_session ON watch_subscriptions(session_id);

-- =============================================================================
-- Audit Log Table (optional, for tracking changes)
-- =============================================================================

CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_type VARCHAR(20) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    key_or_name VARCHAR(1024),
    operation VARCHAR(50) NOT NULL,
    user_id VARCHAR(255),
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_log_store_bucket ON audit_log(store_type, bucket_name);
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);

-- =============================================================================
-- Functions and Triggers
-- =============================================================================

-- Function to update 'updated_at' timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_kv_buckets_updated_at
    BEFORE UPDATE ON kv_buckets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_obj_buckets_updated_at
    BEFORE UPDATE ON obj_buckets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_obj_metadata_updated_at
    BEFORE UPDATE ON obj_metadata
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to get next revision for a key
CREATE OR REPLACE FUNCTION get_next_revision(p_bucket_id UUID, p_key VARCHAR)
RETURNS BIGINT AS $$
DECLARE
    next_rev BIGINT;
BEGIN
    INSERT INTO kv_revision_sequences (bucket_id, key, current_revision)
    VALUES (p_bucket_id, p_key, 1)
    ON CONFLICT (bucket_id, key)
    DO UPDATE SET current_revision = kv_revision_sequences.current_revision + 1
    RETURNING current_revision INTO next_rev;
    
    RETURN next_rev;
END;
$$ LANGUAGE plpgsql;

-- Function to notify watchers of changes
CREATE OR REPLACE FUNCTION notify_change()
RETURNS TRIGGER AS $$
DECLARE
    payload JSONB;
BEGIN
    payload = jsonb_build_object(
        'table', TG_TABLE_NAME,
        'action', TG_OP,
        'timestamp', CURRENT_TIMESTAMP
    );
    
    IF TG_OP = 'DELETE' THEN
        payload = payload || jsonb_build_object('old', row_to_json(OLD));
    ELSE
        payload = payload || jsonb_build_object('new', row_to_json(NEW));
    END IF;
    
    PERFORM pg_notify('store_changes', payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers for notifications
CREATE TRIGGER notify_kv_entry_change
    AFTER INSERT OR UPDATE OR DELETE ON kv_entries
    FOR EACH ROW EXECUTE FUNCTION notify_change();

CREATE TRIGGER notify_obj_metadata_change
    AFTER INSERT OR UPDATE OR DELETE ON obj_metadata
    FOR EACH ROW EXECUTE FUNCTION notify_change();
