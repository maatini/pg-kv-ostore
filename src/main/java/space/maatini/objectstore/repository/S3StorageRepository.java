package space.maatini.objectstore.repository;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import space.maatini.objectstore.entity.ObjMetadata;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
@jakarta.inject.Named("s3")
public class S3StorageRepository implements ObjectStorageRepository {

    private static final Logger LOG = Logger.getLogger(S3StorageRepository.class);

    @Inject
    S3AsyncClient s3;

    @Override
    public Uni<ObjMetadata> save(String bucketName, ObjMetadata metadata, Multi<byte[]> dataStream, int chunkSize,
            long maxSize) {

        // Prepare for hashing and size counting
        AtomicLong size = new AtomicLong(0);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(metadata.digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            return Uni.createFrom().failure(e);
        }

        // Convert Multi<byte[]> to Publisher<ByteBuffer> and intercept for
        // hashing/sizing
        Multi<ByteBuffer> byteBufferMulti = dataStream.map(bytes -> {
            size.addAndGet(bytes.length);
            digest.update(bytes);
            return ByteBuffer.wrap(bytes);
        });

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(metadata.name)
                .contentType(metadata.contentType)
                .metadata(metadata.headers)
                .build();

        // Upload to S3
        return Uni.createFrom()
                .completionStage(s3.putObject(putRequest,
                        AsyncRequestBody.fromPublisher(new FlowToRsPublisher<>(byteBufferMulti))))
                .map(response -> {
                    metadata.size = size.get();
                    metadata.chunkCount = 0; // S3 doesn't use chunks in our tracking sense
                    metadata.digest = HexFormat.of().formatHex(digest.digest());
                    // We could also store response.eTag() if we wanted S3's digest
                    return metadata;
                });
    }

    // Adapter to bridge Java Flow (Mutiny) to Reactive Streams (AWS SDK)
    private static class FlowToRsPublisher<T> implements org.reactivestreams.Publisher<T> {
        private final java.util.concurrent.Flow.Publisher<T> flowPub;

        public FlowToRsPublisher(java.util.concurrent.Flow.Publisher<T> flowPub) {
            this.flowPub = flowPub;
        }

        @Override
        public void subscribe(org.reactivestreams.Subscriber<? super T> s) {
            flowPub.subscribe(new java.util.concurrent.Flow.Subscriber<T>() {
                @Override
                public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                    s.onSubscribe(new org.reactivestreams.Subscription() {
                        @Override
                        public void request(long n) {
                            subscription.request(n);
                        }

                        @Override
                        public void cancel() {
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(T item) {
                    s.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    s.onError(throwable);
                }

                @Override
                public void onComplete() {
                    s.onComplete();
                }
            });
        }
    }

    @Override
    public Uni<byte[]> readRange(String bucketName, ObjMetadata metadata, long offset, long length) {
        long end = offset + length - 1;
        String range = "bytes=" + offset + "-" + end;

        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(metadata.name)
                .range(range)
                .build();

        return Uni.createFrom().completionStage(s3.getObject(getRequest, AsyncResponseTransformer.toBytes()))
                .map(response -> response.asByteArray());
    }

    @Override
    public Uni<Void> delete(String bucketName, ObjMetadata metadata) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(metadata.name)
                .build();

        return Uni.createFrom().completionStage(s3.deleteObject(deleteRequest))
                .replaceWithVoid();
    }

    @Override
    public Uni<Boolean> verifyIntegrity(String bucketName, ObjMetadata metadata) {
        // Full read verification is expensive but accurate
        // Alternatively, we could just check HeadObject for existence and size

        // Let's do a full read to be safe and consistent with Postgres impl
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(metadata.name)
                .build();

        return Uni.createFrom().completionStage(s3.getObject(getRequest, AsyncResponseTransformer.toBytes()))
                .map(response -> {
                    byte[] data = response.asByteArray();
                    try {
                        MessageDigest digest = MessageDigest.getInstance(metadata.digestAlgorithm);
                        byte[] computedDigest = digest.digest(data);
                        String computed = HexFormat.of().formatHex(computedDigest);

                        boolean valid = computed.equals(metadata.digest);

                        if (!valid) {
                            LOG.warnf("Integrity check failed for object %s: expected=%s, computed=%s",
                                    metadata.id, metadata.digest, computed);
                        }
                        return valid;
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                })
                .onFailure().recoverWithItem(e -> {
                    LOG.error("Failed to verify integrity for " + metadata.name, e);
                    return false;
                });
    }
}
