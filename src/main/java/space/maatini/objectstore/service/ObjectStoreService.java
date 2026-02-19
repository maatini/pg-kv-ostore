package space.maatini.objectstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.entity.ObjMetadata;
import space.maatini.objectstore.repository.ObjectStorageRepository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import org.jboss.logging.Logger;

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

    @ConfigProperty(name = "objectstore.backend", defaultValue = "postgres")
    String storageBackend;

    @Inject
    @Any
    Instance<ObjectStorageRepository> storageRepositories;

    @Inject
    ObjectWatchService watchService;

    @Inject
    space.maatini.common.util.DatabaseUtils dbUtils;

    @Inject
    space.maatini.common.util.TenantContext tenantContext;

    private ObjectStorageRepository getRepository() {
        return storageRepositories.select(jakarta.enterprise.inject.literal.NamedLiteral.of(storageBackend)).get();
    }

    // ==================== Bucket Operations ====================

    @WithTransaction
    public Uni<ObjBucket> createBucket(ObjBucketDto.CreateRequest request) {

        return dbUtils.setupTenant()
                .flatMap(v -> ObjBucket.existsByName(request.name, tenantContext.getTenantId()))
                .flatMap(exists -> {
                    if (exists) {
                        throw new ConflictException("Object bucket already exists: " + request.name);
                    }

                    ObjBucket bucket = new ObjBucket();
                    bucket.name = request.name;
                    bucket.tenantId = tenantContext.getTenantId();
                    bucket.description = request.description;
                    bucket.chunkSize = request.chunkSize != null ? request.chunkSize : chunkSize;
                    bucket.maxObjectSize = request.maxObjectSize != null ? request.maxObjectSize : maxObjectSize;

                    return bucket.<ObjBucket>persist()
                            .invoke(b -> LOG.infof("Created object bucket: %s (id=%s)", b.name, b.id));
                });
    }

    public Uni<ObjBucket> getBucket(String name) {
        return dbUtils.setupTenant()
                .flatMap(v -> ObjBucket.findByName(name, tenantContext.getTenantId()))
                .onItem().ifNull().failWith(() -> new NotFoundException("Object bucket not found: " + name));
    }

    public Uni<List<ObjBucket>> listBuckets() {
        String tenantId = tenantContext.getTenantId();
        return dbUtils.setupTenant()
                .flatMap(v -> ObjBucket.<ObjBucket>list("tenantId IS NOT DISTINCT FROM ?1", tenantId));
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
                                    objects.stream().map(obj -> getRepository().delete(name, obj)
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

    // @WithTransaction // Removed to allow granular control and avoid long-running
    // Tx
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
                                    return getRepository().delete(bucketName, existing)
                                            .flatMap(d -> existing.delete())
                                            .replaceWithVoid();
                                }
                                return Uni.createFrom().voidItem();
                            })
                            .replaceWith(bucket);
                })
                .flatMap(bucket -> dbUtils.setupTenant().replaceWith(bucket))
                .flatMap(bucket -> {
                    ObjMetadata metadata = new ObjMetadata();
                    metadata.bucketId = bucket.id;
                    metadata.name = objectName;
                    metadata.size = 0L;
                    metadata.chunkCount = 0;
                    metadata.digestAlgorithm = hashAlgorithm;
                    metadata.contentType = contentType;
                    metadata.description = description;
                    metadata.headers = headers;
                    metadata.tenantId = tenantContext.getTenantId();
                    metadata.status = ObjMetadata.Status.UPLOADING;

                    // 1. Persist metadata in INITIAL transaction
                    return Uni.createFrom().deferred(() -> {
                        return Panache.withTransaction(() -> metadata.<ObjMetadata>persist())
                                .flatMap(m -> {
                                    // 2. Perform streaming upload OUTSIDE the metadata transaction
                                    // The repository logic (PostgresChunkRepository) will manage its own chunk
                                    // transactions
                                    return getRepository()
                                            .save(bucketName, (ObjMetadata) m, dataStream, bucket.chunkSize,
                                                    bucket.maxObjectSize)
                                            .flatMap(finalMetadata -> {
                                                // 3. Finalize in a NEW transaction
                                                return Panache.withTransaction(() -> {
                                                    return ObjMetadata.<ObjMetadata>findById(finalMetadata.id)
                                                            .flatMap(m2 -> {
                                                                m2.size = finalMetadata.size;
                                                                m2.chunkCount = finalMetadata.chunkCount;
                                                                m2.digest = finalMetadata.digest;
                                                                m2.status = ObjMetadata.Status.COMPLETED;
                                                                return m2.persist();
                                                            });
                                                })
                                                        .map(fm -> (ObjMetadata) fm)
                                                        .invoke(fm -> LOG.infof(
                                                                "Stored object: %s/%s (size=%d, chunks=%d, digest=%s, status=%s)",
                                                                bucketName, objectName, fm.size,
                                                                fm.chunkCount,
                                                                fm.digest, fm.status))
                                                        .invoke(fm -> watchService
                                                                .notifyChange(ObjMetadataDto.WatchEvent
                                                                        .fromPut(fm, bucketName)));
                                            })
                                            .onFailure().call(e -> {
                                                LOG.errorf(e, "Failed to store object: %s/%s", bucketName, objectName);
                                                // 4. Mark as FAILED on error
                                                return Panache.withTransaction(() -> {
                                                    metadata.status = ObjMetadata.Status.FAILED;
                                                    return metadata.persist();
                                                });
                                            });
                                });
                    });
                });
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
     * Get a range of object data.
     */
    public Uni<byte[]> getObjectRange(String bucketName, String objectName, long offset, long length) {
        return getMetadata(bucketName, objectName)
                .flatMap(metadata -> getRepository().readRange(bucketName, metadata, offset, length));
    }

    /**
     * Get the complete object data (for smaller objects).
     */
    public Uni<byte[]> getObjectData(String bucketName, String objectName) {
        return getMetadata(bucketName, objectName)
                .flatMap(metadata -> getObjectRange(bucketName, objectName, 0, metadata.size));
    }

    @WithTransaction
    public Uni<Void> deleteObject(String bucketName, String objectName) {
        return dbUtils.setupTenant()
                .flatMap(v -> getMetadata(bucketName, objectName))
                .flatMap(metadata -> getRepository().delete(bucketName, metadata)
                        .flatMap(v -> metadata.delete())
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
                .flatMap(metadata -> getRepository().verifyIntegrity(bucketName, metadata));
    }
}
