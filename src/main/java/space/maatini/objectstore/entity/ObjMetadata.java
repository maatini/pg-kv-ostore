package space.maatini.objectstore.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Entity representing object metadata in the Object Store.
 */
@Entity
@Table(name = "obj_metadata")
public class ObjMetadata extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public UUID id;

    @Column(name = "bucket_id", nullable = false)
    public UUID bucketId;

    @Column(name = "name", nullable = false, length = 1024)
    public String name;

    @Column(name = "size", nullable = false)
    public Long size;

    @Column(name = "chunk_count", nullable = false)
    public Integer chunkCount;

    @Column(name = "digest", length = 128)
    public String digest;

    @Column(name = "digest_algorithm", length = 32)
    public String digestAlgorithm = "SHA-256";

    @Column(name = "content_type", length = 255)
    public String contentType;

    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    @Column(name = "headers", columnDefinition = "JSONB")
    @Convert(converter = JsonbMapConverter.class)
    public Map<String, String> headers;

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
    public static Uni<ObjMetadata> findByBucketAndName(UUID bucketId, String name) {
        return find("bucketId = ?1 AND name = ?2", bucketId, name).firstResult();
    }

    public static Uni<List<ObjMetadata>> findByBucket(UUID bucketId) {
        return list("bucketId", bucketId);
    }

    public static Uni<Boolean> existsByBucketAndName(UUID bucketId, String name) {
        return count("bucketId = ?1 AND name = ?2", bucketId, name).map(count -> count > 0);
    }

    public static Uni<Long> deleteByBucket(UUID bucketId) {
        return delete("bucketId", bucketId);
    }
}
