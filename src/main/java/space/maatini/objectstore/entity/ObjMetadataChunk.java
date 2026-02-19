package space.maatini.objectstore.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "obj_metadata_chunks", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "metadata_id", "chunk_index" })
})
public class ObjMetadataChunk extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    public UUID id;

    @Column(name = "metadata_id")
    public UUID metadataId;

    @Column(name = "chunk_index")
    public int chunkIndex;

    @Column(name = "chunk_digest")
    public String chunkDigest;

    public static Uni<List<ObjMetadataChunk>> findOrderedByMetadata(UUID metadataId) {
        return list("metadataId = ?1 ORDER BY chunkIndex ASC", metadataId);
    }

    public static Uni<List<ObjMetadataChunk>> findOrderedByMetadataAndRange(UUID metadataId, int startChunk,
            int endChunk) {
        return list("metadataId = ?1 AND chunkIndex >= ?2 AND chunkIndex <= ?3 ORDER BY chunkIndex ASC",
                metadataId, startChunk, endChunk);
    }
}
