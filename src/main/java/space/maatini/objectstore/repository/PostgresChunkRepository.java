package space.maatini.objectstore.repository;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.entity.ObjMetadata;
import space.maatini.objectstore.entity.ObjSharedChunk;
import space.maatini.objectstore.entity.ObjMetadataChunk;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.ArrayList;

@ApplicationScoped
@jakarta.inject.Named("postgres")
public class PostgresChunkRepository implements ObjectStorageRepository {

    private static final Logger LOG = Logger.getLogger(PostgresChunkRepository.class);

    @Override
    public Uni<ObjMetadata> save(String bucketName, ObjMetadata metadata, Multi<byte[]> dataStream, int chunkSize,
            long maxSize) {
        return processDataStream(metadata, dataStream, chunkSize, maxSize);
    }

    private Uni<ObjMetadata> processDataStream(ObjMetadata metadata, Multi<byte[]> dataStream, int chunkSize,
            long maxSize) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(metadata.digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            return Uni.createFrom().failure(e);
        }

        // State holder for chunking
        class State {
            long totalSize = 0;
            int chunkIndex = 0;
            byte[] leftover = new byte[0];
        }
        State state = new State();

        return dataStream.onItem().transformToUniAndConcatenate(data -> {
            state.totalSize += data.length;
            if (state.totalSize > maxSize) {
                return Uni.createFrom().failure(new ValidationException("Object size exceeds maximum"));
            }
            digest.update(data);

            // Concatenate with leftover
            byte[] combined;
            if (state.leftover.length > 0) {
                combined = new byte[state.leftover.length + data.length];
                System.arraycopy(state.leftover, 0, combined, 0, state.leftover.length);
                System.arraycopy(data, 0, combined, state.leftover.length, data.length);
            } else {
                combined = data;
            }

            int offset = 0;
            List<byte[]> chunksToStore = new ArrayList<>();
            while (combined.length - offset >= chunkSize) {
                byte[] chunkData = new byte[chunkSize];
                System.arraycopy(combined, offset, chunkData, 0, chunkSize);
                chunksToStore.add(chunkData);
                offset += chunkSize;
            }

            state.leftover = new byte[combined.length - offset];
            System.arraycopy(combined, offset, state.leftover, 0, state.leftover.length);

            if (chunksToStore.isEmpty()) {
                LOG.debug("No full chunks in buffer, waiting for more data");
                return Uni.createFrom().voidItem();
            }

            // Sequence chunk storage
            return Multi.createFrom().iterable(chunksToStore)
                    .onItem()
                    .transformToUniAndConcatenate(
                            chunkData -> storeChunk(metadata, state.chunkIndex++, chunkData, metadata.digestAlgorithm))
                    .collect().last();
        }).collect().asList()
                .flatMap(v -> {
                    // Store final leftover
                    if (state.leftover.length > 0) {
                        return storeChunk(metadata, state.chunkIndex++, state.leftover, metadata.digestAlgorithm);
                    }
                    return Uni.createFrom().voidItem();
                })
                .map(v -> {
                    metadata.size = state.totalSize;
                    metadata.chunkCount = state.chunkIndex;
                    metadata.digest = HexFormat.of().formatHex(digest.digest());
                    return metadata;
                });
    }

    private Uni<Void> storeChunk(ObjMetadata metadata, int index, byte[] data, String algorithm) {
        String digest;
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            digest = HexFormat.of().formatHex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            return Uni.createFrom().failure(e);
        }

        // Each chunk in its own transaction
        return Panache.withTransaction(() -> ObjSharedChunk.findByDigest(digest)
                .flatMap(existing -> {
                    if (existing == null) {
                        ObjSharedChunk newShared = new ObjSharedChunk();
                        newShared.digest = digest;
                        newShared.data = data;
                        newShared.size = data.length;
                        return newShared.persist().replaceWith(digest);
                    }
                    return Uni.createFrom().item(digest);
                })
                .flatMap(finalDigest -> {
                    ObjMetadataChunk mapping = new ObjMetadataChunk();
                    mapping.metadataId = metadata.id;
                    mapping.chunkIndex = index;
                    mapping.chunkDigest = (String) finalDigest;
                    return mapping.persist();
                })).replaceWithVoid();
    }

    @Override
    public Uni<byte[]> readRange(String bucketName, ObjMetadata metadata, long offset, long length) {
        if (offset < 0 || length < 1 || offset >= metadata.size) {
            return Uni.createFrom().failure(new ValidationException("Invalid range request"));
        }

        long actualLength = Math.min(length, metadata.size - offset);

        return space.maatini.objectstore.entity.ObjBucket.findById(metadata.bucketId)
                .flatMap(bucketEntity -> {
                    space.maatini.objectstore.entity.ObjBucket bucket = (space.maatini.objectstore.entity.ObjBucket) bucketEntity;
                    int chunkSize = bucket.chunkSize;

                    int startChunk = (int) (offset / chunkSize);
                    int endChunk = (int) ((offset + actualLength - 1) / chunkSize);

                    return ObjMetadataChunk.findOrderedByMetadataAndRange(metadata.id, startChunk, endChunk)
                            .flatMap(mappings -> {
                                ByteArrayOutputStream output = new ByteArrayOutputStream((int) actualLength);

                                // Process mappings sequentially to avoids concurrent session issues
                                return Multi.createFrom().iterable(mappings)
                                        .onItem()
                                        .transformToUniAndConcatenate(
                                                m -> ObjSharedChunk.<ObjSharedChunk>findById(m.chunkDigest)
                                                        .invoke(chunk -> {
                                                            if (chunk == null) {
                                                                LOG.warnf("Chunk not found for digest: %s",
                                                                        m.chunkDigest);
                                                                return;
                                                            }
                                                            int chunkIdx = mappings.indexOf(m);
                                                            long currentPos = (long) (startChunk + chunkIdx)
                                                                    * chunkSize;
                                                            long chunkEnd = currentPos + chunk.size;

                                                            long sliceStart = Math.max(offset, currentPos);
                                                            long sliceEnd = Math.min(offset + actualLength, chunkEnd);

                                                            if (sliceStart < sliceEnd) {
                                                                int startOffsetInChunk = (int) (sliceStart
                                                                        - currentPos);
                                                                int lengthInChunk = (int) (sliceEnd - sliceStart);
                                                                output.write(chunk.data, startOffsetInChunk,
                                                                        lengthInChunk);
                                                            }
                                                        }))
                                        .collect().asList()
                                        .map(v -> {
                                            byte[] result = output.toByteArray();
                                            return result;
                                        });
                            });
                });
    }

    @Override
    public Uni<Void> delete(String bucketName, ObjMetadata metadata) {
        return ObjMetadataChunk.delete("metadataId", metadata.id)
                .replaceWithVoid();
    }

    @Override
    public Uni<Boolean> verifyIntegrity(String bucketName, ObjMetadata metadata) {
        return ObjMetadataChunk.findOrderedByMetadata(metadata.id)
                .flatMap(mappings -> {
                    try {
                        MessageDigest digest = MessageDigest.getInstance(metadata.digestAlgorithm);

                        // Process chunks sequentially for integrity check
                        return Multi.createFrom().iterable(mappings)
                                .onItem()
                                .transformToUniAndConcatenate(
                                        m -> ObjSharedChunk.<ObjSharedChunk>findById(m.chunkDigest)
                                                .invoke(chunk -> digest.update(chunk.data)))
                                .collect().asList()
                                .map(v -> {
                                    String computed = HexFormat.of().formatHex(digest.digest());
                                    return computed.equals(metadata.digest);
                                });
                    } catch (NoSuchAlgorithmException e) {
                        return Uni.createFrom().failure(e);
                    }
                })
                .onItem().ifNull().continueWith(metadata.size == 0);
    }
}
