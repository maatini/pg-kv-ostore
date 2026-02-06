package space.maatini.objectstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.entity.ObjChunk;
import space.maatini.objectstore.entity.ObjMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for Object Store operations.
 */
@ApplicationScoped
public class ObjectStoreService {

    private static final Logger LOG = Logger.getLogger(ObjectStoreService.class);

    @ConfigProperty(name = "objectstore.chunk-size", defaultValue = "1048576")
    int chunkSize;

    @ConfigProperty(name = "objectstore.max-object-size", defaultValue = "1073741824")
    long maxObjectSize;

    @ConfigProperty(name = "objectstore.hash-algorithm", defaultValue = "SHA-256")
    String hashAlgorithm;

    @Inject
    ObjectWatchService watchService;

    // ==================== Bucket Operations ====================

    @Transactional
    public ObjBucket createBucket(ObjBucketDto.CreateRequest request) {
        LOG.debugf("Creating object bucket: %s", request.name);

        if (ObjBucket.existsByName(request.name)) {
            throw new ConflictException("Object bucket already exists: " + request.name);
        }

        ObjBucket bucket = new ObjBucket();
        bucket.name = request.name;
        bucket.description = request.description;
        bucket.chunkSize = request.chunkSize != null ? request.chunkSize : chunkSize;
        bucket.maxObjectSize = request.maxObjectSize != null ? request.maxObjectSize : maxObjectSize;
        bucket.persist();

        LOG.infof("Created object bucket: %s (id=%s)", bucket.name, bucket.id);
        return bucket;
    }

    public ObjBucket getBucket(String name) {
        ObjBucket bucket = ObjBucket.findByName(name);
        if (bucket == null) {
            throw new NotFoundException("Object bucket not found: " + name);
        }
        return bucket;
    }

    public List<ObjBucket> listBuckets() {
        return ObjBucket.listAll();
    }

    @Transactional
    public ObjBucket updateBucket(String name, ObjBucketDto.UpdateRequest request) {
        ObjBucket bucket = getBucket(name);

        if (request.description != null) {
            bucket.description = request.description;
        }
        if (request.chunkSize != null) {
            bucket.chunkSize = request.chunkSize;
        }
        if (request.maxObjectSize != null) {
            bucket.maxObjectSize = request.maxObjectSize;
        }

        bucket.persist();
        LOG.infof("Updated object bucket: %s", bucket.name);
        return bucket;
    }

    @Transactional
    public void deleteBucket(String name) {
        ObjBucket bucket = getBucket(name);

        // Delete all objects first
        List<ObjMetadata> objects = ObjMetadata.findByBucket(bucket.id);
        for (ObjMetadata obj : objects) {
            ObjChunk.deleteByMetadata(obj.id);
            obj.delete();
        }
        LOG.infof("Deleted %d objects from bucket: %s", objects.size(), name);

        bucket.delete();
        LOG.infof("Deleted object bucket: %s", name);
    }

    // ==================== Object Operations ====================

    @Transactional
    public ObjMetadata putObject(
            String bucketName,
            String objectName,
            InputStream data,
            String contentType,
            String description,
            Map<String, String> headers) throws IOException {

        ObjBucket bucket = getBucket(bucketName);
        LOG.debugf("Putting object: %s/%s", bucketName, objectName);

        // Check if object already exists and delete it
        ObjMetadata existing = ObjMetadata.findByBucketAndName(bucket.id, objectName);
        if (existing != null) {
            ObjChunk.deleteByMetadata(existing.id);
            existing.delete();
            LOG.debugf("Replaced existing object: %s/%s", bucketName, objectName);
        }

        // Read and chunk the data
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(hashAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Hash algorithm not available: " + hashAlgorithm, e);
        }

        ByteArrayOutputStream totalBuffer = new ByteArrayOutputStream();
        byte[] buffer = new byte[bucket.chunkSize];
        int bytesRead;
        int chunkIndex = 0;
        long totalSize = 0;

        // Create metadata first (we'll update it after)
        ObjMetadata metadata = new ObjMetadata();
        metadata.bucketId = bucket.id;
        metadata.name = objectName;
        metadata.size = 0L;
        metadata.chunkCount = 0;
        metadata.digestAlgorithm = hashAlgorithm;
        metadata.contentType = contentType;
        metadata.description = description;
        metadata.headers = headers;
        metadata.persist();

        // Read chunks and store
        ByteArrayOutputStream chunkBuffer = new ByteArrayOutputStream();
        while ((bytesRead = data.read(buffer)) != -1) {
            chunkBuffer.write(buffer, 0, bytesRead);
            digest.update(buffer, 0, bytesRead);
            totalSize += bytesRead;

            // Check size limit
            if (totalSize > bucket.maxObjectSize) {
                // Rollback: delete metadata and any chunks
                ObjChunk.deleteByMetadata(metadata.id);
                metadata.delete();
                throw new ValidationException(String.format(
                        "Object size exceeds maximum (%d bytes)", bucket.maxObjectSize));
            }

            // If chunk buffer is full, store chunk
            while (chunkBuffer.size() >= bucket.chunkSize) {
                byte[] chunkData = chunkBuffer.toByteArray();
                byte[] toStore = new byte[bucket.chunkSize];
                System.arraycopy(chunkData, 0, toStore, 0, bucket.chunkSize);

                storeChunk(metadata, chunkIndex++, toStore);

                // Keep remaining bytes
                byte[] remaining = new byte[chunkData.length - bucket.chunkSize];
                System.arraycopy(chunkData, bucket.chunkSize, remaining, 0, remaining.length);
                chunkBuffer.reset();
                chunkBuffer.write(remaining, 0, remaining.length);
            }
        }

        // Store final partial chunk if any
        if (chunkBuffer.size() > 0) {
            storeChunk(metadata, chunkIndex++, chunkBuffer.toByteArray());
        }

        // Update metadata with final values
        metadata.size = totalSize;
        metadata.chunkCount = chunkIndex;
        metadata.digest = HexFormat.of().formatHex(digest.digest());
        metadata.persist();

        LOG.infof("Stored object: %s/%s (size=%d, chunks=%d, digest=%s)",
                bucketName, objectName, totalSize, chunkIndex, metadata.digest);

        // Notify watchers
        watchService.notifyChange(ObjMetadataDto.WatchEvent.fromPut(metadata, bucketName));

        return metadata;
    }

    private void storeChunk(ObjMetadata metadata, int index, byte[] data) {
        ObjChunk chunk = new ObjChunk();
        chunk.metadataId = metadata.id;
        chunk.chunkIndex = index;
        chunk.data = data;
        chunk.size = data.length;

        // Calculate chunk digest
        try {
            MessageDigest md = MessageDigest.getInstance(hashAlgorithm);
            chunk.digest = HexFormat.of().formatHex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("Could not calculate chunk digest", e);
        }

        chunk.persist();
    }

    public ObjMetadata getMetadata(String bucketName, String objectName) {
        ObjBucket bucket = getBucket(bucketName);
        ObjMetadata metadata = ObjMetadata.findByBucketAndName(bucket.id, objectName);

        if (metadata == null) {
            throw new NotFoundException(String.format("Object not found: %s/%s", bucketName, objectName));
        }

        return metadata;
    }

    public List<ObjMetadata> listObjects(String bucketName) {
        ObjBucket bucket = getBucket(bucketName);
        return ObjMetadata.findByBucket(bucket.id);
    }

    /**
     * Get a streaming iterator over object chunks.
     */
    public ChunkIterator getObjectChunks(String bucketName, String objectName) {
        ObjMetadata metadata = getMetadata(bucketName, objectName);
        return new ChunkIterator(metadata.id, metadata.chunkCount);
    }

    /**
     * Get the complete object data (for smaller objects).
     */
    public byte[] getObjectData(String bucketName, String objectName) throws IOException {
        ObjMetadata metadata = getMetadata(bucketName, objectName);
        List<ObjChunk> chunks = ObjChunk.findByMetadataOrdered(metadata.id);

        ByteArrayOutputStream output = new ByteArrayOutputStream((int) (long) metadata.size);
        for (ObjChunk chunk : chunks) {
            output.write(chunk.data);
        }

        return output.toByteArray();
    }

    @Transactional
    public void deleteObject(String bucketName, String objectName) {
        ObjBucket bucket = getBucket(bucketName);
        ObjMetadata metadata = ObjMetadata.findByBucketAndName(bucket.id, objectName);

        if (metadata == null) {
            throw new NotFoundException(String.format("Object not found: %s/%s", bucketName, objectName));
        }

        // Delete chunks first
        long deletedChunks = ObjChunk.deleteByMetadata(metadata.id);
        metadata.delete();

        LOG.infof("Deleted object: %s/%s (chunks=%d)", bucketName, objectName, deletedChunks);

        // Notify watchers
        watchService.notifyChange(ObjMetadataDto.WatchEvent.fromDelete(bucketName, objectName));
    }

    /**
     * Verify object integrity by comparing stored digest with computed digest.
     */
    public boolean verifyIntegrity(String bucketName, String objectName) {
        ObjMetadata metadata = getMetadata(bucketName, objectName);
        List<ObjChunk> chunks = ObjChunk.findByMetadataOrdered(metadata.id);

        try {
            MessageDigest digest = MessageDigest.getInstance(metadata.digestAlgorithm);
            for (ObjChunk chunk : chunks) {
                digest.update(chunk.data);
            }
            String computed = HexFormat.of().formatHex(digest.digest());
            boolean valid = computed.equals(metadata.digest);

            if (!valid) {
                LOG.warnf("Integrity check failed for %s/%s: expected=%s, computed=%s",
                        bucketName, objectName, metadata.digest, computed);
            }

            return valid;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Hash algorithm not available: " + metadata.digestAlgorithm, e);
        }
    }

    /**
     * Iterator for streaming object chunks.
     */
    public static class ChunkIterator {
        private final java.util.UUID metadataId;
        private final int totalChunks;
        private int currentIndex = 0;

        public ChunkIterator(java.util.UUID metadataId, int totalChunks) {
            this.metadataId = metadataId;
            this.totalChunks = totalChunks;
        }

        public boolean hasNext() {
            return currentIndex < totalChunks;
        }

        public ObjChunk next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            ObjChunk chunk = ObjChunk.findByMetadataAndIndex(metadataId, currentIndex++);
            if (chunk == null) {
                throw new IllegalStateException("Missing chunk at index " + (currentIndex - 1));
            }
            return chunk;
        }

        public int getTotalChunks() {
            return totalChunks;
        }

        public int getCurrentIndex() {
            return currentIndex;
        }
    }
}
