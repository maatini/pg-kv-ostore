package space.maatini.objectstore.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import java.util.List;
import java.util.UUID;

/**
 * Entity representing a chunk of object data.
 * Large objects are split into multiple chunks for efficient storage and
 * retrieval.
 */
@Entity
@Table(name = "obj_chunks")
@NamedQueries({
        @NamedQuery(name = "ObjChunk.findByMetadataOrderedByIndex", query = "SELECT c FROM ObjChunk c WHERE c.metadataId = :metadataId ORDER BY c.chunkIndex ASC"),
        @NamedQuery(name = "ObjChunk.deleteByMetadataId", query = "DELETE FROM ObjChunk c WHERE c.metadataId = :metadataId")
})
public class ObjChunk extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public UUID id;

    @Column(name = "metadata_id", nullable = false)
    public UUID metadataId;

    @Column(name = "chunk_index", nullable = false)
    public Integer chunkIndex;

    @Column(name = "data", nullable = false, columnDefinition = "BYTEA")
    public byte[] data;

    @Column(name = "size", nullable = false)
    public Integer size;

    @Column(name = "digest", length = 128)
    public String digest;

    // Panache Finder Methods
    public static Uni<List<ObjChunk>> findByMetadataOrdered(UUID metadataId) {
        return list("metadataId = ?1 ORDER BY chunkIndex ASC", metadataId);
    }

    public static Uni<ObjChunk> findByMetadataAndIndex(UUID metadataId, int chunkIndex) {
        return find("metadataId = ?1 AND chunkIndex = ?2", metadataId, chunkIndex).firstResult();
    }

    public static Uni<List<ObjChunk>> findOrderedByMetadataAndRange(UUID metadataId, int startIndex, int endIndex) {
        return list("metadataId = ?1 AND chunkIndex >= ?2 AND chunkIndex <= ?3 ORDER BY chunkIndex ASC",
                metadataId, startIndex, endIndex);
    }

    public static Uni<Long> deleteByMetadata(UUID metadataId) {
        return delete("metadataId", metadataId);
    }
}
