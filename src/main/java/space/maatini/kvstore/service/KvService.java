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

    @Inject
    space.maatini.common.util.DatabaseUtils dbUtils;

    @Inject
    space.maatini.common.util.TenantContext tenantContext;

    // ==================== Bucket Operations ====================

    @WithTransaction
    public Uni<KvBucket> createBucket(KvBucketDto.CreateRequest request) {
        LOG.debugf("Creating bucket: %s", request.name);

        return dbUtils.setupTenant()
                .flatMap(v -> KvBucket.existsByName(request.name, tenantContext.getTenantId()))
                .flatMap(exists -> {
                    if (exists) {
                        throw new ConflictException(
                                "Bucket already exists: " + request.name);
                    }

                    KvBucket bucket = new KvBucket();
                    bucket.name = request.name;
                    bucket.tenantId = tenantContext.getTenantId();
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
        return dbUtils.setupTenant()
                .flatMap(v -> KvBucket.findByName(name, tenantContext.getTenantId()))
                .onItem().ifNull()
                .failWith(() -> new NotFoundException("Bucket not found: " + name));
    }

    public Uni<List<KvBucket>> listBuckets() {
        String tenantId = tenantContext.getTenantId();
        return dbUtils.setupTenant()
                .flatMap(v -> KvBucket.<KvBucket>list("tenantId IS NOT DISTINCT FROM ?1", tenantId));
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
                    byte[] value;
                    if (request.base64) {
                        try {
                            value = Base64.getDecoder().decode(request.value);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom().failure(new ValidationException("Invalid base64 value"));
                        }
                    } else {
                        value = request.value != null ? request.value.getBytes() : new byte[0];
                    }

                    if (value.length > bucket.maxValueSize) {
                        return Uni.createFrom()
                                .failure(new ValidationException(
                                        String.format("Value size (%d bytes) exceeds maximum (%d bytes)", value.length,
                                                bucket.maxValueSize)));
                    }

                    final byte[] finalValue = value;

                    return dbUtils.setupTenant().flatMap(v -> {
                        return KvEntry.getSession().flatMap(session -> {
                            String sql = "SELECT CAST(kv_put(CAST(:bucket_name AS varchar), CAST(:key AS text), CAST(:value AS bytea), CAST(:ttl AS bigint), CAST(:max_history AS integer)) AS text)";
                            return session.createNativeQuery(sql, String.class)
                                    .setParameter("bucket_name", bucketName)
                                    .setParameter("key", key)
                                    .setParameter("value", finalValue)
                                    .setParameter("ttl", request.ttlSeconds != null ? request.ttlSeconds : -1L)
                                    .setParameter("max_history", -1)
                                    .getSingleResult()
                                    .map(result -> {
                                        try {
                                            io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject(
                                                    result);
                                            KvEntry entry = new KvEntry();
                                            entry.id = java.util.UUID.fromString(obj.getString("id"));
                                            entry.bucketId = java.util.UUID.fromString(obj.getString("bucket_id"));
                                            entry.key = key;
                                            entry.value = finalValue;
                                            entry.revision = obj.getLong("revision");
                                            entry.operation = Operation.PUT;
                                            entry.tenantId = tenantContext.getTenantId();
                                            entry.createdAt = OffsetDateTime.parse(obj.getString("created_at"));
                                            String expiresAtStr = obj.getString("expires_at");
                                            if (expiresAtStr != null) {
                                                entry.expiresAt = OffsetDateTime.parse(expiresAtStr);
                                            }
                                            return entry;
                                        } catch (Exception e) {
                                            throw new RuntimeException("Failed to parse DB result", e);
                                        }
                                    });
                        });
                    });
                });
    }

    @WithTransaction
    public Uni<KvEntry> cas(String bucketName, String key, KvEntryDto.PutRequest request, long expectedRevision) {
        return getBucket(bucketName)
                .flatMap(bucket -> {
                    byte[] value;
                    if (request.base64) {
                        try {
                            value = Base64.getDecoder().decode(request.value);
                        } catch (IllegalArgumentException e) {
                            return Uni.createFrom()
                                    .failure(new ValidationException("Invalid base64 value: " + e.getMessage()));
                        }
                    } else {
                        value = request.value != null ? request.value.getBytes() : new byte[0];
                    }

                    if (value == null) {
                        return Uni.createFrom().failure(new ValidationException("Value cannot be null for CAS"));
                    }

                    if (value.length > bucket.maxValueSize) {
                        return Uni.createFrom()
                                .failure(new ValidationException(
                                        String.format("Value size (%d bytes) exceeds maximum (%d bytes)", value.length,
                                                bucket.maxValueSize)));
                    }

                    final byte[] finalValue = value;

                    return dbUtils.setupTenant().flatMap(v -> {
                        return KvEntry.getSession().flatMap(session -> {
                            String sql = "SELECT CAST(kv_cas(CAST(:bucket_name AS varchar), CAST(:key AS text), CAST(:value AS bytea), CAST(:expected_rev AS bigint), CAST(:ttl AS bigint), CAST(:max_history AS integer)) AS text)";
                            return session.createNativeQuery(sql, String.class)
                                    .setParameter("bucket_name", bucketName)
                                    .setParameter("key", key)
                                    .setParameter("value", finalValue)
                                    .setParameter("expected_rev", expectedRevision)
                                    .setParameter("ttl", request.ttlSeconds != null ? request.ttlSeconds : -1L)
                                    .setParameter("max_history", -1)
                                    .getSingleResult()
                                    .map(result -> {
                                        try {
                                            io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject(
                                                    result);
                                            KvEntry entry = new KvEntry();
                                            entry.id = java.util.UUID.fromString(obj.getString("id"));
                                            entry.bucketId = java.util.UUID.fromString(obj.getString("bucket_id"));
                                            entry.key = key;
                                            entry.value = finalValue;
                                            entry.revision = obj.getLong("revision");
                                            entry.operation = Operation.PUT;
                                            entry.tenantId = tenantContext.getTenantId();
                                            entry.createdAt = OffsetDateTime.parse(obj.getString("created_at"));
                                            String expiresAtStr = obj.getString("expires_at");
                                            if (expiresAtStr != null) {
                                                entry.expiresAt = OffsetDateTime.parse(expiresAtStr);
                                            }
                                            return entry;
                                        } catch (Exception e) {
                                            throw new RuntimeException("Failed to parse DB result", e);
                                        }
                                    })
                                    .onFailure().transform(t -> {
                                        if (t.getMessage().contains("P0003")
                                                || t.getMessage().contains("CAS Failure")) {
                                            return new ConflictException(t.getMessage());
                                        }
                                        return t;
                                    });
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
        return dbUtils.setupTenant().flatMap(v -> {
            return KvEntry.getSession().flatMap(session -> {
                String sql = "SELECT CAST(kv_delete(CAST(:bucket_name AS varchar), CAST(:key AS text)) AS text)";
                return session.createNativeQuery(sql)
                        .setParameter("bucket_name", bucketName)
                        .setParameter("key", key)
                        .getSingleResult()
                        .map(result -> {
                            try {
                                io.vertx.core.json.JsonObject obj = new io.vertx.core.json.JsonObject(
                                        result.toString());
                                KvEntry entry = new KvEntry();
                                entry.id = java.util.UUID.fromString(obj.getString("id"));
                                entry.bucketId = java.util.UUID.fromString(obj.getString("bucket_id"));
                                entry.key = key;
                                entry.value = null;
                                entry.revision = obj.getLong("revision");
                                entry.operation = Operation.DELETE;
                                entry.createdAt = OffsetDateTime.parse(obj.getString("created_at"));
                                LOG.debugf("Deleted key: %s/%s (revision=%d)", bucketName, key, entry.revision);
                                return entry;
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to parse DB result or Key not found", e);
                            }
                        })
                        .onFailure().transform(t -> {
                            if (t.getMessage().contains("P0002") || t.getMessage().contains("Key not found")) {
                                return new NotFoundException("Key not found: " + bucketName + "/" + key);
                            }
                            return t;
                        });
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
