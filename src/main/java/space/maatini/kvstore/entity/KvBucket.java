package space.maatini.kvstore.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Entity representing a Key-Value store bucket.
 * A bucket is a logical namespace for grouping related keys.
 */
@Entity
@Table(name = "kv_buckets")
public class KvBucket extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public UUID id;

    @Column(name = "name", unique = true, nullable = false, length = 255)
    public String name;

    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    @Column(name = "max_value_size")
    public Integer maxValueSize = 1048576; // 1MB default

    @Column(name = "max_history_per_key")
    public Integer maxHistoryPerKey = 100;

    @Column(name = "ttl_seconds")
    public Long ttlSeconds;

    @Column(name = "created_at")
    public OffsetDateTime createdAt;

    @Column(name = "updated_at")
    public OffsetDateTime updatedAt;

    @PrePersist
    public void onCreate() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    public void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }

    // Panache Finder Methods
    public static KvBucket findByName(String name) {
        return find("name", name).firstResult();
    }

    public static boolean existsByName(String name) {
        return count("name", name) > 0;
    }
}
