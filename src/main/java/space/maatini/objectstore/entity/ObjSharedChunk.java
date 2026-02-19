package space.maatini.objectstore.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Entity representing a unique chunk of data in the content-addressable
 * storage.
 */
@Entity
@Table(name = "obj_shared_chunks")
public class ObjSharedChunk extends PanacheEntityBase {

    @Id
    @Column(name = "digest", nullable = false, length = 128)
    public String digest;

    @Column(name = "data", nullable = false, columnDefinition = "BYTEA")
    public byte[] data;

    @Column(name = "size", nullable = false)
    public Integer size;

    @Column(name = "created_at", updatable = false)
    public OffsetDateTime createdAt;

    @PrePersist
    public void onCreate() {
        createdAt = OffsetDateTime.now();
    }

    public static Uni<ObjSharedChunk> findByDigest(String digest) {
        return findById(digest);
    }
}
