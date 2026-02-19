package space.maatini.objectstore.repository;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.entity.ObjChunk;
import space.maatini.objectstore.entity.ObjMetadata;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

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
            List<Uni<Void>> chunkOps = new java.util.ArrayList<>();
            while (combined.length - offset >= chunkSize) {
                byte[] chunkData = new byte[chunkSize];
                System.arraycopy(combined, offset, chunkData, 0, chunkSize);
                chunkOps.add(storeChunk(metadata, state.chunkIndex++, chunkData, metadata.digestAlgorithm));
                offset += chunkSize;
            }

            state.leftover = new byte[combined.length - offset];
            System.arraycopy(combined, offset, state.leftover, 0, state.leftover.length);

            if (chunkOps.isEmpty())
                return Uni.createFrom().voidItem();
            return Uni.combine().all().unis(chunkOps).discardItems();
        }).collect().last()
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
        ObjChunk chunk = new ObjChunk();
        chunk.metadataId = metadata.id;
        chunk.chunkIndex = index;
        chunk.data = data;
        chunk.size = data.length;

        // Calculate chunk digest
        try {
            MessageDigest md = MessageDigest.getInstance(algorithm);
            chunk.digest = HexFormat.of().formatHex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("Could not calculate chunk digest", e);
        }

        return chunk.<ObjChunk>persist().replaceWithVoid();
    }

    @Override
    public Uni<byte[]> readRange(String bucketName, ObjMetadata metadata, long offset, long length) {
        if (offset < 0 || length < 0 || offset >= metadata.size) {
            throw new ValidationException("Invalid range: offset=" + offset + ", length=" + length);
        }

        long actualLength = Math.min(length, metadata.size - offset);
        if (actualLength == 0) {
            return Uni.createFrom().item(new byte[0]);
        }

        // Fetch chunkSize from bucket.
        return space.maatini.objectstore.entity.ObjBucket.findById(metadata.bucketId)
                .flatMap(bucketEntity -> {
                    space.maatini.objectstore.entity.ObjBucket bucket = (space.maatini.objectstore.entity.ObjBucket) bucketEntity;
                    int chunkSize = bucket.chunkSize;

                    int startChunk = (int) (offset / chunkSize);
                    int endChunk = (int) ((offset + actualLength - 1) / chunkSize);

                    return ObjChunk.findOrderedByMetadataAndRange(metadata.id, startChunk, endChunk)
                            .map(chunks -> {
                                ByteArrayOutputStream output = new ByteArrayOutputStream((int) actualLength);
                                long currentPos = (long) startChunk * chunkSize;

                                for (ObjChunk chunk : chunks) {
                                    long chunkEnd = currentPos + chunk.size;

                                    long sliceStart = Math.max(offset, currentPos);
                                    long sliceEnd = Math.min(offset + actualLength, chunkEnd);

                                    if (sliceStart < sliceEnd) {
                                        int startOffsetInChunk = (int) (sliceStart - currentPos);
                                        int lengthInChunk = (int) (sliceEnd - sliceStart);
                                        output.write(chunk.data, startOffsetInChunk, lengthInChunk);
                                    }
                                    currentPos = chunkEnd;
                                }
                                return output.toByteArray();
                            });
                });
    }

    @Override
    public Uni<Void> delete(String bucketName, ObjMetadata metadata) {
        return ObjChunk.deleteByMetadata(metadata.id)
                .replaceWithVoid();
    }

    @Override
    public Uni<Boolean> verifyIntegrity(String bucketName, ObjMetadata metadata) {
        return ObjChunk.findByMetadataOrdered(metadata.id)
                .map(chunks -> {
                    try {
                        MessageDigest digest = MessageDigest.getInstance(metadata.digestAlgorithm);
                        for (ObjChunk chunk : chunks) {
                            digest.update(chunk.data);
                        }
                        String computed = HexFormat.of().formatHex(digest.digest());
                        boolean valid = computed.equals(metadata.digest);

                        if (!valid) {
                            LOG.warnf("Integrity check failed for object %s: expected=%s, computed=%s",
                                    metadata.id, metadata.digest, computed);
                        }

                        return valid;
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("Hash algorithm not available: " + metadata.digestAlgorithm,
                                e);
                    }
                });
    }
}
