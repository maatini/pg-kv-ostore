package space.maatini.objectstore.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Entity representing an Object Store bucket.
 * A bucket is a logical namespace for grouping related objects.
 */
@Entity
@Table(name = "obj_buckets")
public class ObjBucket extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public UUID id;

    @Column(name = "name", nullable = false, length = 255)
    public String name;

    @Column(name = "tenant_id", length = 255)
    public String tenantId;

    @Column(name = "description", columnDefinition = "TEXT")
    public String description;

    @Column(name = "chunk_size")
    public Integer chunkSize = 1048576; // 1MB default

    @Column(name = "max_object_size")
    public Long maxObjectSize = 1073741824L; // 1GB default

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
    public static Uni<ObjBucket> findByName(String name, String tenantId) {
        return find("name = ?1 and tenantId IS NOT DISTINCT FROM ?2", name, tenantId).firstResult();
    }

    public static Uni<Boolean> existsByName(String name, String tenantId) {
        return count("name = ?1 and tenantId IS NOT DISTINCT FROM ?2", name, tenantId).map(count -> count > 0);
    }
}
