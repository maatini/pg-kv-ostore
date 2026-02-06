package space.maatini.kvstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.kvstore.dto.KvBucketDto;
import space.maatini.kvstore.dto.KvEntryDto;
import space.maatini.kvstore.entity.KvBucket;
import space.maatini.kvstore.entity.KvEntry;
import space.maatini.kvstore.entity.KvEntry.Operation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for Key-Value store operations.
 */
@ApplicationScoped
public class KvService {

    private static final Logger LOG = Logger.getLogger(KvService.class);

    @ConfigProperty(name = "kv.max-value-size", defaultValue = "1048576")
    int maxValueSize;

    @ConfigProperty(name = "kv.max-history-size", defaultValue = "100")
    int maxHistorySize;

    @Inject
    KvWatchService watchService;

    // ==================== Bucket Operations ====================

    @Transactional
    public KvBucket createBucket(KvBucketDto.CreateRequest request) {
        LOG.debugf("Creating bucket: %s", request.name);

        if (KvBucket.existsByName(request.name)) {
            throw new ConflictException("Bucket already exists: " + request.name);
        }

        KvBucket bucket = new KvBucket();
        bucket.name = request.name;
        bucket.description = request.description;
        bucket.maxValueSize = request.maxValueSize != null ? request.maxValueSize : maxValueSize;
        bucket.maxHistoryPerKey = request.maxHistoryPerKey != null ? request.maxHistoryPerKey : maxHistorySize;
        bucket.ttlSeconds = request.ttlSeconds;
        bucket.persist();

        LOG.infof("Created bucket: %s (id=%s)", bucket.name, bucket.id);
        return bucket;
    }

    public KvBucket getBucket(String name) {
        KvBucket bucket = KvBucket.findByName(name);
        if (bucket == null) {
            throw new NotFoundException("Bucket not found: " + name);
        }
        return bucket;
    }

    public List<KvBucket> listBuckets() {
        return KvBucket.listAll();
    }

    @Transactional
    public KvBucket updateBucket(String name, KvBucketDto.UpdateRequest request) {
        KvBucket bucket = getBucket(name);

        if (request.description != null) {
            bucket.description = request.description;
        }
        if (request.maxValueSize != null) {
            bucket.maxValueSize = request.maxValueSize;
        }
        if (request.maxHistoryPerKey != null) {
            bucket.maxHistoryPerKey = request.maxHistoryPerKey;
        }
        if (request.ttlSeconds != null) {
            bucket.ttlSeconds = request.ttlSeconds;
        }

        bucket.persist();
        LOG.infof("Updated bucket: %s", bucket.name);
        return bucket;
    }

    @Transactional
    public void deleteBucket(String name) {
        KvBucket bucket = getBucket(name);

        // Delete all entries first (cascades via FK, but log it)
        long deletedEntries = KvEntry.purgeByBucket(bucket.id);
        LOG.infof("Deleted %d entries from bucket: %s", deletedEntries, name);

        bucket.delete();
        LOG.infof("Deleted bucket: %s", name);
    }

    // ==================== Key-Value Operations ====================

    @Transactional
    public KvEntry put(String bucketName, String key, KvEntryDto.PutRequest request) {
        KvBucket bucket = getBucket(bucketName);

        // Decode value
        byte[] value;
        if (request.base64) {
            try {
                value = Base64.getDecoder().decode(request.value);
            } catch (IllegalArgumentException e) {
                throw new ValidationException("Invalid base64 value");
            }
        } else {
            value = request.value != null ? request.value.getBytes() : new byte[0];
        }

        // Validate size
        if (value.length > bucket.maxValueSize) {
            throw new ValidationException(String.format(
                    "Value size (%d bytes) exceeds maximum (%d bytes)",
                    value.length, bucket.maxValueSize));
        }

        // Get next revision
        long currentRevision = KvEntry.getLatestRevision(bucket.id, key);
        long nextRevision = currentRevision + 1;

        // Create entry
        KvEntry entry = new KvEntry();
        entry.bucketId = bucket.id;
        entry.key = key;
        entry.value = value;
        entry.revision = nextRevision;
        entry.operation = Operation.PUT;

        // Set TTL if specified
        if (request.ttlSeconds != null && request.ttlSeconds > 0) {
            entry.expiresAt = OffsetDateTime.now().plusSeconds(request.ttlSeconds);
        } else if (bucket.ttlSeconds != null && bucket.ttlSeconds > 0) {
            entry.expiresAt = OffsetDateTime.now().plusSeconds(bucket.ttlSeconds);
        }

        entry.persist();

        // Cleanup old revisions if needed
        cleanupOldRevisions(bucket.id, key, bucket.maxHistoryPerKey);

        LOG.debugf("Put key: %s/%s (revision=%d, size=%d)", bucketName, key, nextRevision, value.length);

        // Notify watchers
        watchService.notifyChange(KvEntryDto.WatchEvent.from(entry, bucketName));

        return entry;
    }

    public KvEntry get(String bucketName, String key) {
        KvBucket bucket = getBucket(bucketName);
        KvEntry entry = KvEntry.findLatest(bucket.id, key);

        if (entry == null) {
            throw new NotFoundException(String.format("Key not found: %s/%s", bucketName, key));
        }

        // Check if deleted
        if (entry.operation == Operation.DELETE) {
            throw new NotFoundException(String.format("Key deleted: %s/%s", bucketName, key));
        }

        return entry;
    }

    public KvEntry getRevision(String bucketName, String key, Long revision) {
        KvBucket bucket = getBucket(bucketName);
        KvEntry entry = KvEntry.findByRevision(bucket.id, key, revision);

        if (entry == null) {
            throw new NotFoundException(String.format(
                    "Revision not found: %s/%s@%d", bucketName, key, revision));
        }

        return entry;
    }

    public List<KvEntry> getHistory(String bucketName, String key, int limit) {
        KvBucket bucket = getBucket(bucketName);
        return KvEntry.findHistory(bucket.id, key, limit > 0 ? limit : bucket.maxHistoryPerKey);
    }

    public List<String> listKeys(String bucketName) {
        KvBucket bucket = getBucket(bucketName);
        return KvEntry.findAllKeys(bucket.id);
    }

    @Transactional
    public KvEntry delete(String bucketName, String key) {
        KvBucket bucket = getBucket(bucketName);

        // Check if key exists
        KvEntry latest = KvEntry.findLatest(bucket.id, key);
        if (latest == null) {
            throw new NotFoundException(String.format("Key not found: %s/%s", bucketName, key));
        }

        // Create delete marker
        long nextRevision = latest.revision + 1;
        KvEntry deleteMarker = new KvEntry();
        deleteMarker.bucketId = bucket.id;
        deleteMarker.key = key;
        deleteMarker.value = null;
        deleteMarker.revision = nextRevision;
        deleteMarker.operation = Operation.DELETE;
        deleteMarker.persist();

        LOG.debugf("Deleted key: %s/%s (revision=%d)", bucketName, key, nextRevision);

        // Notify watchers
        watchService.notifyChange(KvEntryDto.WatchEvent.from(deleteMarker, bucketName));

        return deleteMarker;
    }

    @Transactional
    public long purge(String bucketName, String key) {
        KvBucket bucket = getBucket(bucketName);
        long deleted = KvEntry.deleteByKey(bucket.id, key);
        LOG.infof("Purged key: %s/%s (deleted %d revisions)", bucketName, key, deleted);
        return deleted;
    }

    @Transactional
    public long purgeBucket(String bucketName) {
        KvBucket bucket = getBucket(bucketName);
        long deleted = KvEntry.purgeByBucket(bucket.id);
        LOG.infof("Purged bucket: %s (deleted %d entries)", bucketName, deleted);
        return deleted;
    }

    // ==================== Private Methods ====================

    @Transactional
    void cleanupOldRevisions(java.util.UUID bucketId, String key, int maxHistory) {
        List<KvEntry> entries = KvEntry.find(
                "bucketId = ?1 AND key = ?2 ORDER BY revision DESC",
                bucketId, key).list();

        if (entries.size() > maxHistory) {
            List<KvEntry> toDelete = entries.subList(maxHistory, entries.size());
            for (KvEntry entry : toDelete) {
                entry.delete();
            }
            LOG.debugf("Cleaned up %d old revisions for key: %s", toDelete.size(), key);
        }
    }
}
