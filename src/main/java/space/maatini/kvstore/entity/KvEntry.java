package space.maatini.kvstore.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Entity representing a Key-Value entry with revision history support.
 */
@Entity
@Table(name = "kv_entries")
@NamedQueries({
        @NamedQuery(name = "KvEntry.findLatestByBucketAndKey", query = "SELECT e FROM KvEntry e WHERE e.bucketId = :bucketId AND e.key = :key ORDER BY e.revision DESC"),
        @NamedQuery(name = "KvEntry.findHistoryByBucketAndKey", query = "SELECT e FROM KvEntry e WHERE e.bucketId = :bucketId AND e.key = :key ORDER BY e.revision DESC"),
        @NamedQuery(name = "KvEntry.deleteByBucketIdAndKey", query = "DELETE FROM KvEntry e WHERE e.bucketId = :bucketId AND e.key = :key"),
        @NamedQuery(name = "KvEntry.deleteByBucketId", query = "DELETE FROM KvEntry e WHERE e.bucketId = :bucketId"),
        @NamedQuery(name = "KvEntry.findAllKeysByBucket", query = "SELECT DISTINCT e.key FROM KvEntry e WHERE e.bucketId = :bucketId"),
        @NamedQuery(name = "KvEntry.findExpired", query = "SELECT e FROM KvEntry e WHERE e.expiresAt IS NOT NULL AND e.expiresAt < :now")
})
public class KvEntry extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public UUID id;

    @Column(name = "bucket_id", nullable = false)
    public UUID bucketId;

    @Column(name = "key", nullable = false, length = 1024)
    public String key;

    @Column(name = "value", columnDefinition = "BYTEA")
    public byte[] value;

    @Column(name = "revision", nullable = false)
    public Long revision = 1L;

    @Column(name = "operation", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    public Operation operation = Operation.PUT;

    @Column(name = "created_at")
    public OffsetDateTime createdAt;

    @Column(name = "expires_at")
    public OffsetDateTime expiresAt;

    public enum Operation {
        PUT, DELETE, PURGE
    }

    @PrePersist
    public void onCreate() {
        createdAt = OffsetDateTime.now();
    }

    // Panache Finder Methods
    public static KvEntry findLatest(UUID bucketId, String key) {
        return find("bucketId = ?1 AND key = ?2 ORDER BY revision DESC", bucketId, key)
                .firstResult();
    }

    public static KvEntry findByRevision(UUID bucketId, String key, Long revision) {
        return find("bucketId = ?1 AND key = ?2 AND revision = ?3", bucketId, key, revision)
                .firstResult();
    }

    public static List<KvEntry> findHistory(UUID bucketId, String key, int limit) {
        return find("bucketId = ?1 AND key = ?2 ORDER BY revision DESC", bucketId, key)
                .page(0, limit)
                .list();
    }

    public static List<String> findAllKeys(UUID bucketId) {
        return getEntityManager()
                .createNamedQuery("KvEntry.findAllKeysByBucket", String.class)
                .setParameter("bucketId", bucketId)
                .getResultList();
    }

    public static long deleteByKey(UUID bucketId, String key) {
        return delete("bucketId = ?1 AND key = ?2", bucketId, key);
    }

    public static long purgeByBucket(UUID bucketId) {
        return delete("bucketId", bucketId);
    }

    public static long getLatestRevision(UUID bucketId, String key) {
        Long rev = getEntityManager()
                .createQuery("SELECT MAX(e.revision) FROM KvEntry e WHERE e.bucketId = :bucketId AND e.key = :key",
                        Long.class)
                .setParameter("bucketId", bucketId)
                .setParameter("key", key)
                .getSingleResult();
        return rev != null ? rev : 0L;
    }
}
