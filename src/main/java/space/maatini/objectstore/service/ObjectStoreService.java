package space.maatini.objectstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.entity.ObjChunk;
import space.maatini.objectstore.entity.ObjMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service for Object Store operations.
 */
@ApplicationScoped
@WithSession
public class ObjectStoreService {

    private static final Logger LOG = Logger.getLogger(ObjectStoreService.class);

    @ConfigProperty(name = "objectstore.chunk-size", defaultValue = "1048576")
    int chunkSize;

    @ConfigProperty(name = "objectstore.max-object-size", defaultValue = "1073741824")
    long maxObjectSize;

    @ConfigProperty(name = "objectstore.hash-algorithm", defaultValue = "SHA-256")
    String hashAlgorithm;

    @Inject
    ObjectWatchService watchService;

    // ==================== Bucket Operations ====================

    @WithTransaction
    public Uni<ObjBucket> createBucket(ObjBucketDto.CreateRequest request) {
        LOG.debugf("Creating object bucket: %s", request.name);

        return ObjBucket.existsByName(request.name)
                .flatMap(exists -> {
                    if (exists) {
                        throw new ConflictException("Object bucket already exists: " + request.name);
                    }

                    ObjBucket bucket = new ObjBucket();
                    bucket.name = request.name;
                    bucket.description = request.description;
                    bucket.chunkSize = request.chunkSize != null ? request.chunkSize : chunkSize;
                    bucket.maxObjectSize = request.maxObjectSize != null ? request.maxObjectSize : maxObjectSize;

                    return bucket.<ObjBucket>persist()
                            .invoke(b -> LOG.infof("Created object bucket: %s (id=%s)", b.name, b.id));
                });
    }

    public Uni<ObjBucket> getBucket(String name) {
        return ObjBucket.findByName(name)
                .onItem().ifNull().failWith(() -> new NotFoundException("Object bucket not found: " + name));
    }

    public Uni<List<ObjBucket>> listBuckets() {
        return ObjBucket.listAll();
    }

    @WithTransaction
    public Uni<ObjBucket> updateBucket(String name, ObjBucketDto.UpdateRequest request) {
        return getBucket(name)
                .flatMap(bucket -> {
                    if (request.description != null) {
                        bucket.description = request.description;
                    }
                    if (request.chunkSize != null) {
                        bucket.chunkSize = request.chunkSize;
                    }
                    if (request.maxObjectSize != null) {
                        bucket.maxObjectSize = request.maxObjectSize;
                    }

                    return bucket.<ObjBucket>persist()
                            .invoke(b -> LOG.infof("Updated object bucket: %s", b.name));
                });
    }

    @WithTransaction
    public Uni<Void> deleteBucket(String name) {
        return getBucket(name)
                .flatMap(bucket -> ObjMetadata.findByBucket(bucket.id)
                        .flatMap(objects -> {
                            if (objects.isEmpty())
                                return Uni.createFrom().item(objects);
                            return Uni.combine().all().unis(
                                    objects.stream().map(obj -> ObjChunk.deleteByMetadata(obj.id)
                                            .flatMap(d -> obj.delete())).collect(Collectors.toList()))
                                    .discardItems().replaceWith(objects);
                        })
                        .invoke(objects -> LOG.infof("Deleted %d objects from bucket: %s", objects.size(), name))
                        .replaceWith(bucket))
                .flatMap(bucket -> bucket.delete())
                .invoke(() -> LOG.infof("Deleted object bucket: %s", name))
                .replaceWithVoid();
    }

    // ==================== Object Operations ====================

    @WithTransaction
    public Uni<ObjMetadata> putObject(
            String bucketName,
            String objectName,
            Multi<byte[]> dataStream,
            String contentType,
            String description,
            Map<String, String> headers) {

        return getBucket(bucketName)
                .flatMap(bucket -> {
                    // Check if object already exists and delete it
                    return ObjMetadata.findByBucketAndName(bucket.id, objectName)
                            .flatMap(existing -> {
                                if (existing != null) {
                                    return ObjChunk.deleteByMetadata(existing.id)
                                            .flatMap(d -> existing.delete())
                                            .replaceWithVoid();
                                }
                                return Uni.createFrom().voidItem();
                            })
                            .replaceWith(bucket);
                })
                .flatMap(bucket -> {
                    // This is a simplified reactive chunking.
                    // In a real scenario, we might want to use a stateful transformation or
                    // collect.
                    // For now, let's accumulate into chunks.

                    ObjMetadata metadata = new ObjMetadata();
                    metadata.bucketId = bucket.id;
                    metadata.name = objectName;
                    metadata.size = 0L;
                    metadata.chunkCount = 0;
                    metadata.digestAlgorithm = hashAlgorithm;
                    metadata.contentType = contentType;
                    metadata.description = description;
                    metadata.headers = headers;

                    return metadata.<ObjMetadata>persist()
                            .flatMap(m -> {
                                // Process stream and store chunks
                                // Note: This is a complex reactive pipe.
                                // We'll use a local state to track chunks.
                                return processDataStream(m, dataStream, bucket.chunkSize, bucket.maxObjectSize)
                                        .flatMap(finalMetadata -> finalMetadata.<ObjMetadata>persist())
                                        .invoke(fm -> LOG.infof("Stored object: %s/%s (size=%d, chunks=%d, digest=%s)",
                                                bucketName, objectName, fm.size, fm.chunkCount, fm.digest))
                                        .invoke(fm -> watchService
                                                .notifyChange(ObjMetadataDto.WatchEvent.fromPut(fm, bucketName)));
                            });
                });
    }

    private Uni<ObjMetadata> processDataStream(ObjMetadata metadata, Multi<byte[]> dataStream, int chunkSize,
            long maxSize) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(hashAlgorithm);
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
                chunkOps.add(storeChunk(metadata, state.chunkIndex++, chunkData));
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
                        return storeChunk(metadata, state.chunkIndex++, state.leftover);
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

    private Uni<Void> storeChunk(ObjMetadata metadata, int index, byte[] data) {
        ObjChunk chunk = new ObjChunk();
        chunk.metadataId = metadata.id;
        chunk.chunkIndex = index;
        chunk.data = data;
        chunk.size = data.length;

        // Calculate chunk digest
        try {
            MessageDigest md = MessageDigest.getInstance(hashAlgorithm);
            chunk.digest = HexFormat.of().formatHex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("Could not calculate chunk digest", e);
        }

        return chunk.<ObjChunk>persist().replaceWithVoid();
    }

    public Uni<ObjMetadata> getMetadata(String bucketName, String objectName) {
        return getBucket(bucketName)
                .flatMap(bucket -> ObjMetadata.findByBucketAndName(bucket.id, objectName)
                        .onItem().ifNull().failWith(() -> new NotFoundException(
                                String.format("Object not found: %s/%s", bucketName, objectName))));
    }

    public Uni<List<ObjMetadata>> listObjects(String bucketName) {
        return getBucket(bucketName)
                .flatMap(bucket -> ObjMetadata.findByBucket(bucket.id));
    }

    /**
     * Get a streaming Multi of object chunks.
     */
    public Multi<ObjChunk> getObjectChunks(String bucketName, String objectName) {
        return getMetadata(bucketName, objectName)
                .onItem()
                .transformToMulti(metadata -> ObjChunk.findByMetadataOrdered(metadata.id)
                        .onItem().transformToMulti(chunks -> Multi.createFrom().iterable(chunks)));
    }

    /**
     * Get the complete object data (for smaller objects).
     */
    public Uni<byte[]> getObjectData(String bucketName, String objectName) {
        return getMetadata(bucketName, objectName)
                .flatMap(metadata -> ObjChunk.findByMetadataOrdered(metadata.id)
                        .map(chunks -> {
                            ByteArrayOutputStream output = new ByteArrayOutputStream((int) (long) metadata.size);
                            for (ObjChunk chunk : chunks) {
                                try {
                                    output.write(chunk.data);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            return output.toByteArray();
                        }));
    }

    @WithTransaction
    public Uni<Void> deleteObject(String bucketName, String objectName) {
        return getMetadata(bucketName, objectName)
                .flatMap(metadata -> ObjChunk.deleteByMetadata(metadata.id)
                        .flatMap(deletedChunks -> metadata.delete())
                        .invoke(() -> LOG.infof("Deleted object: %s/%s", bucketName, objectName))
                        .invoke(() -> watchService
                                .notifyChange(ObjMetadataDto.WatchEvent.fromDelete(bucketName, objectName))))
                .replaceWithVoid();
    }

    /**
     * Verify object integrity by comparing stored digest with computed digest.
     */
    public Uni<Boolean> verifyIntegrity(String bucketName, String objectName) {
        return getMetadata(bucketName, objectName)
                .flatMap(metadata -> ObjChunk.findByMetadataOrdered(metadata.id)
                        .map(chunks -> {
                            try {
                                MessageDigest digest = MessageDigest.getInstance(metadata.digestAlgorithm);
                                for (ObjChunk chunk : chunks) {
                                    digest.update(chunk.data);
                                }
                                String computed = HexFormat.of().formatHex(digest.digest());
                                boolean valid = computed.equals(metadata.digest);

                                if (!valid) {
                                    LOG.warnf("Integrity check failed for %s/%s: expected=%s, computed=%s",
                                            bucketName, objectName, metadata.digest, computed);
                                }

                                return valid;
                            } catch (NoSuchAlgorithmException e) {
                                throw new RuntimeException("Hash algorithm not available: " + metadata.digestAlgorithm,
                                        e);
                            }
                        }));
    }
}
