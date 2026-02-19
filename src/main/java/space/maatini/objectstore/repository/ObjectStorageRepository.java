package space.maatini.objectstore.repository;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import space.maatini.objectstore.entity.ObjMetadata;

/**
 * Interface for object storage backends.
 */
public interface ObjectStorageRepository {

    /**
     * Save object data.
     *
     * @param bucketName The bucket name.
     * @param metadata   The object metadata (bucket, name, etc.).
     * @param dataStream The data stream.
     * @param chunkSize  The configured chunk size.
     * @param maxSize    The configured max size.
     * @return A Uni resolving to the updated metadata (size, chunkCount, digest).
     */
    Uni<ObjMetadata> save(String bucketName, ObjMetadata metadata, Multi<byte[]> dataStream, int chunkSize,
            long maxSize);

    /**
     * Read a range of bytes from the object.
     *
     * @param bucketName The bucket name.
     * @param metadata   The object metadata.
     * @param offset     The start offset.
     * @param length     The length to read.
     * @return A Uni resolving to the byte array.
     */
    Uni<byte[]> readRange(String bucketName, ObjMetadata metadata, long offset, long length);

    /**
     * Delete the object data.
     *
     * @param bucketName The bucket name.
     * @param metadata   The object metadata.
     * @return A Uni resolving when deletion is complete.
     */
    Uni<Void> delete(String bucketName, ObjMetadata metadata);

    /**
     * Verify the integrity of the stored object.
     *
     * @param bucketName The bucket name.
     * @param metadata   The object metadata.
     * @return A Uni resolving to true if valid, false otherwise.
     */
    Uni<Boolean> verifyIntegrity(String bucketName, ObjMetadata metadata);
}
