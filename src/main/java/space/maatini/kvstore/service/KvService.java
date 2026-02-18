package space.maatini.kvstore.service;

import io.smallrye.mutiny.Uni;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for Key-Value store operations.
 */
@ApplicationScoped
@WithSession
public class KvService {

    private static final Logger LOG = Logger.getLogger(KvService.class);

    @ConfigProperty(name = "kv.max-value-size", defaultValue = "1048576")
    int maxValueSize;

    @ConfigProperty(name = "kv.max-history-size", defaultValue = "100")
    int maxHistorySize;

    @Inject
    KvWatchService watchService;

    // ==================== Bucket Operations ====================

    @WithTransaction
    public Uni<KvBucket> createBucket(KvBucketDto.CreateRequest request) {
        LOG.debugf("Creating bucket: %s", request.name);

        return KvBucket.existsByName(request.name)
                .flatMap(exists -> {
                    if (exists) {
                        throw new ConflictException(
                                "Bucket already exists: " + request.name);
                    }

                    KvBucket bucket = new KvBucket();
                    bucket.name = request.name;
                    bucket.description = request.description;
                    bucket.maxValueSize = request.maxValueSize != null ? request.maxValueSize : maxValueSize;
                    bucket.maxHistoryPerKey = request.maxHistoryPerKey != null ? request.maxHistoryPerKey
                            : maxHistorySize;
                    bucket.ttlSeconds = request.ttlSeconds;

                    return bucket.<KvBucket>persist()
                            .invoke(b -> LOG.infof("Created bucket: %s (id=%s)", b.name, b.id));
                });
    }

    public Uni<KvBucket> getBucket(String name) {
        return KvBucket.findByName(name)
                .onItem().ifNull()
                .failWith(() -> new NotFoundException("Bucket not found: " + name));
    }

    public Uni<List<KvBucket>> listBuckets() {
        return KvBucket.listAll();
    }

    @WithTransaction
    public Uni<KvBucket> updateBucket(String name, KvBucketDto.UpdateRequest request) {
        return getBucket(name)
                .flatMap(bucket -> {
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

                    return bucket.<KvBucket>persist()
                            .invoke(b -> LOG.infof("Updated bucket: %s", b.name));
                });
    }

    @WithTransaction
    public Uni<Void> deleteBucket(String name) {
        return getBucket(name)
                .flatMap(bucket -> KvEntry.purgeByBucket(bucket.id)
                        .invoke(deleted -> LOG.infof("Deleted %d entries from bucket: %s", deleted, name))
                        .replaceWith(bucket))
                .flatMap(bucket -> bucket.delete())
                .invoke(() -> LOG.infof("Deleted bucket: %s", name))
                .replaceWithVoid();
    }

    // ==================== Key-Value Operations ====================

    @WithTransaction
    public Uni<KvEntry> put(String bucketName, String key, KvEntryDto.PutRequest request) {
        return getBucket(bucketName)
                .flatMap(bucket -> {
                    // Decode value
                    byte[] value;
                    if (request.base64) {
                        try {
                            value = Base64.getDecoder().decode(request.value);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().failure(
                                    new ValidationException("Invalid base64 value"));
                        }
                    } else {
                        value = request.value != null ? request.value.getBytes() : new byte[0];
                    }

                    // Validate size
                    if (value.length > bucket.maxValueSize) {
                        return Uni.createFrom()
                                .failure(new ValidationException(String.format(
                                        "Value size (%d bytes) exceeds maximum (%d bytes)",
                                        value.length, bucket.maxValueSize)));
                    }

                    final byte[] finalValue = value;

                    // Call atomic stored procedure
                    return KvEntry.getSession().flatMap(session -> {
                        String sql = "SELECT CAST(kv_put(CAST(:bucket_name AS varchar), CAST(:key AS varchar), CAST(:value AS bytea), CAST(:ttl AS bigint), CAST(:max_history AS integer)) AS text)";
                        return session.createNativeQuery(sql)
                                .setParameter("bucket_name", bucketName)
                                .setParameter("key", key)
                                .setParameter("value", finalValue)
                                .setParameter("ttl", request.ttlSeconds != null ? request.ttlSeconds : -1L)
                                .setParameter("max_history", -1) // Use bucket default from DB (-1)
                                .getSingleResult()
                                .map(result -> {
                                    // Result is JSONB string
                                    try {
                                        // result might be String or PGObject
                                        String json = result.toString();
                                        io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject(json);

                                        KvEntry entry = new KvEntry();
                                        entry.id = java.util.UUID.fromString(obj.getString("id"));
                                        entry.bucketId = java.util.UUID.fromString(obj.getString("bucket_id"));
                                        entry.key = key;
                                        entry.value = finalValue;
                                        entry.revision = obj.getLong("revision");
                                        entry.operation = Operation.PUT;
                                        // Parse timestamps if needed, or just set now() for response
                                        // Response uses entry.createdAt
                                        // obj.getString("created_at") -> Parse
                                        entry.createdAt = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
                                                .parse(obj.getString("created_at"), OffsetDateTime::from);
                                        // expiresAt is not returned by SP explicitely in JSON unless we add it
                                        // But reasonable to leave null or fetch if needed.
                                        // For response, created_at is key.

                                        return entry;
                                    } catch (Exception e) {
                                        throw new RuntimeException("Failed to parse DB result", e);
                                    }
                                });
                    });
                });
    }

    public Uni<KvEntry> get(String bucketName, String key) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.findLatest(bucket.id, key)
                        .onItem().ifNull()
                        .failWith(() -> new NotFoundException(String.format("Key not found: %s/%s", bucketName, key)))
                        .invoke(entry -> {
                            if (entry.operation == Operation.DELETE) {
                                throw new NotFoundException(String.format("Key deleted: %s/%s", bucketName, key));
                            }
                        }));
    }

    public Uni<KvEntry> getRevision(String bucketName, String key, Long revision) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.findByRevision(bucket.id, key, revision)
                        .onItem().ifNull().failWith(() -> new NotFoundException(String.format(
                                "Revision not found: %s/%s@%d", bucketName, key, revision))));
    }

    public Uni<List<KvEntry>> getHistory(String bucketName, String key, int limit) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.findHistory(bucket.id, key, limit > 0 ? limit : bucket.maxHistoryPerKey));
    }

    public Uni<List<String>> listKeys(String bucketName) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.findAllKeys(bucket.id));
    }

    @WithTransaction
    public Uni<KvEntry> delete(String bucketName, String key) {
        // Validation: Check if bucket/key exists logic inside SP or keep here?
        // SP checks if key exists.
        // But getBucket(bucketName) is good for standard 404 on bucket.
        // SP throws exception if bucket/key not found.
        // If I call SP directly, exception mapping is harder (PSQLException).
        // I will keep getBucket.

        return KvEntry.getSession().flatMap(session -> {
            String sql = "SELECT CAST(kv_delete(CAST(:bucket_name AS varchar), CAST(:key AS varchar)) AS text)";
            return session.createNativeQuery(sql)
                    .setParameter("bucket_name", bucketName)
                    .setParameter("key", key)
                    .getSingleResult()
                    .map(result -> {
                        try {
                            String json = result.toString();
                            io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject(json);

                            KvEntry entry = new KvEntry();
                            entry.id = java.util.UUID.fromString(obj.getString("id"));
                            entry.bucketId = java.util.UUID.fromString(obj.getString("bucket_id"));
                            entry.key = key;
                            entry.value = null;
                            entry.revision = obj.getLong("revision");
                            entry.operation = Operation.DELETE;
                            entry.createdAt = java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
                                    .parse(obj.getString("created_at"), OffsetDateTime::from);

                            LOG.debugf("Deleted key: %s/%s (revision=%d)", bucketName, key, entry.revision);
                            return entry;
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to parse DB result or Key not found", e);
                        }
                    })
                    // Map specific DB errors to NotFound?
                    .onFailure().transform(t -> {
                        // If SP raises "Key not found", we get exception.
                        if (t.getMessage().contains("P0002") || t.getMessage().contains("Key not found")) {
                            return new NotFoundException("Key not found: " + bucketName + "/" + key);
                        }
                        return t;
                    });
        });
    }

    @WithTransaction
    public Uni<Long> purge(String bucketName, String key) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.deleteByKey(bucket.id, key)
                        .invoke(deleted -> LOG.infof("Purged key: %s/%s (deleted %d revisions)", bucketName, key,
                                deleted)));
    }

    @WithTransaction
    public Uni<Long> purgeBucket(String bucketName) {
        return getBucket(bucketName)
                .flatMap(bucket -> KvEntry.purgeByBucket(bucket.id)
                        .invoke(deleted -> LOG.infof("Purged bucket: %s (deleted %d entries)", bucketName, deleted)));
    }

    // ==================== Private Methods ====================

    Uni<Void> cleanupOldRevisions(java.util.UUID bucketId, String key, int maxHistory) {
        return KvEntry.find("bucketId = ?1 AND key = ?2 ORDER BY revision DESC", bucketId, key)
                .list()
                .flatMap(entries -> {
                    if (entries.size() > maxHistory) {
                        List<KvEntry> toDelete = entries.stream()
                                .skip(maxHistory)
                                .map(e -> (KvEntry) e)
                                .collect(Collectors.toList());
                        return Uni.combine().all().unis(
                                toDelete.stream().map(e -> e.delete()).collect(Collectors.toList())).discardItems()
                                .invoke(() -> LOG.debugf("Cleaned up %d old revisions for key: %s", toDelete.size(),
                                        key));
                    }
                    return Uni.createFrom().voidItem();
                });
    }
}
