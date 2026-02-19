package space.maatini.objectstore.service;

import io.smallrye.mutiny.Multi;
import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.entity.ObjMetadataChunk;
import space.maatini.objectstore.entity.ObjSharedChunk;
import space.maatini.objectstore.entity.ObjMetadata;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.quarkus.hibernate.reactive.panache.common.WithSession;
import io.quarkus.hibernate.reactive.panache.Panache;

/**
 * Unit tests for ObjectStoreService.
 */
@QuarkusTest
@WithSession
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ObjectStoreServiceTest {

    @Inject
    ObjectStoreService objectStoreService;

    @InjectMock
    ObjectWatchService watchService;

    private static final String TEST_BUCKET = "unit-test-objects";
    private static final String TEST_OBJECT = "test-file.txt";
    private static final byte[] TEST_DATA = "This is test content for the object store.".getBytes();

    private static UUID testBucketId;

    @BeforeEach
    @RunOnVertxContext
    void setUp(UniAsserter asserter) {
        Mockito.reset(watchService);
        asserter.execute(() -> Panache.withTransaction(() -> ObjMetadataChunk.deleteAll()
                .chain(() -> ObjSharedChunk.deleteAll())
                .chain(() -> ObjMetadata.deleteAll())
                .chain(() -> ObjBucket.deleteAll())));
        asserter.execute(() -> {
            ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
            request.name = TEST_BUCKET;
            request.chunkSize = 1024; // 1KB chunks for testing
            request.maxObjectSize = 1024L * 1024 * 1024;
            return objectStoreService.createBucket(request)
                    .invoke(bucket -> testBucketId = bucket.id)
                    .replaceWithVoid();
        });
    }

    // ==================== Bucket Tests ====================

    @Test
    @Order(1)
    @RunOnVertxContext
    void testCreateBucket_Success(UniAsserter asserter) {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = "new-obj-bucket";
        request.description = "Test bucket";

        asserter.assertThat(() -> objectStoreService.createBucket(request), bucket -> {
            assertNotNull(bucket);
            assertEquals("new-obj-bucket", bucket.name);
        });
    }

    @Test
    @Order(2)
    @RunOnVertxContext
    void testCreateBucket_DuplicateName(UniAsserter asserter) {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = TEST_BUCKET;

        asserter.assertFailedWith(() -> objectStoreService.createBucket(request), ConflictException.class);
    }

    @Test
    @Order(3)
    @RunOnVertxContext
    void testGetBucket_Success(UniAsserter asserter) {
        asserter.assertThat(() -> objectStoreService.getBucket(TEST_BUCKET), bucket -> {
            assertNotNull(bucket);
            assertEquals(TEST_BUCKET, bucket.name);
            assertEquals(testBucketId, bucket.id);
        });
    }

    @Test
    @Order(4)
    @RunOnVertxContext
    void testGetBucket_NotFound(UniAsserter asserter) {
        asserter.assertFailedWith(() -> objectStoreService.getBucket("nonexistent"), NotFoundException.class);
    }

    @Test
    @Order(5)
    @RunOnVertxContext
    void testListBuckets(UniAsserter asserter) {
        asserter.assertThat(() -> objectStoreService.listBuckets(), buckets -> {
            assertNotNull(buckets);
            assertFalse(buckets.isEmpty());
            assertTrue(buckets.stream().anyMatch(b -> b.name.equals(TEST_BUCKET)));
        });
    }

    @Test
    @Order(6)
    @RunOnVertxContext
    void testUpdateBucket(UniAsserter asserter) {
        ObjBucketDto.UpdateRequest request = new ObjBucketDto.UpdateRequest();
        request.description = "Updated description";
        request.chunkSize = 2048;

        asserter.assertThat(() -> objectStoreService.updateBucket(TEST_BUCKET, request), bucket -> {
            assertNotNull(bucket);
            assertEquals("Updated description", bucket.description);
            assertEquals(2048, bucket.chunkSize);
        });
    }

    // ==================== Object Upload Tests ====================

    @Test
    @Order(5)
    @RunOnVertxContext
    void testPutObject_Success(UniAsserter asserter) {
        asserter.assertThat(() -> objectStoreService.putObject(
                TEST_BUCKET, TEST_OBJECT, Multi.createFrom().item(TEST_DATA), "text/plain", null, null),
                metadata -> {
                    assertNotNull(metadata);
                    assertEquals(TEST_OBJECT, metadata.name);
                    assertEquals(TEST_DATA.length, metadata.size);
                    assertEquals(ObjMetadata.Status.COMPLETED, metadata.status);
                });
        asserter.execute(() -> verify(watchService).notifyChange(any(ObjMetadataDto.WatchEvent.class)));
    }

    @Test
    @Order(11)
    @RunOnVertxContext
    void testPutObject_VerifyDigest(UniAsserter asserter) throws NoSuchAlgorithmException {
        asserter.assertThat(() -> objectStoreService.putObject(
                TEST_BUCKET, "digest-test.txt", Multi.createFrom().item(TEST_DATA), "text/plain", null, null),
                metadata -> {
                    // Calculate expected digest
                    MessageDigest md = null;
                    try {
                        md = MessageDigest.getInstance("SHA-256");
                    } catch (NoSuchAlgorithmException e) {
                        fail("SHA-256 algorithm not found", e);
                    }
                    String expectedDigest = HexFormat.of().formatHex(md.digest(TEST_DATA));

                    assertEquals(expectedDigest, metadata.digest);
                });
    }

    @Test
    @Order(12)
    @RunOnVertxContext
    void testPutObject_WithChunking(UniAsserter asserter) {
        // Create data larger than chunk size (chunk size is 2048 from update test)
        byte[] largeData = new byte[5000];
        new Random().nextBytes(largeData);

        asserter.assertThat(() -> objectStoreService.putObject(
                TEST_BUCKET, "chunked-file.bin", Multi.createFrom().item(largeData), "application/octet-stream", null,
                null), metadata -> {
                    assertNotNull(metadata);
                    assertEquals(largeData.length, metadata.size);
                    assertTrue(metadata.chunkCount > 1);
                });
    }

    @Test
    @Order(13)
    @RunOnVertxContext
    void testPutObject_ReplaceExisting(UniAsserter asserter) {
        // First upload
        byte[] data1 = "First version".getBytes();
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, "replace-test.txt",
                Multi.createFrom().item(data1), "text/plain", null, null));

        // Replace with new data
        byte[] data2 = "Second version".getBytes();
        asserter.assertThat(() -> objectStoreService.putObject(
                TEST_BUCKET, "replace-test.txt",
                Multi.createFrom().item(data2), "text/plain", null, null), metadata -> {
                    assertEquals(data2.length, metadata.size);
                });

        // Verify content was replaced
        asserter.assertThat(() -> objectStoreService.getObjectData(TEST_BUCKET, "replace-test.txt"), retrieved -> {
            assertArrayEquals(data2, retrieved);
        });
    }

    @Test
    @Order(14)
    @RunOnVertxContext
    void testPutObject_TooLarge(UniAsserter asserter) {
        // Create data larger than maxObjectSize (1GB default, but bucket has 1GB from
        // creation)
        // Wait, the bucket creation in test 1 sets 1GB.
        // Let's use a smaller max size for this test by creating a new bucket.
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "small-limit-bucket";
        bucketRequest.maxObjectSize = 1024L;

        asserter.execute(() -> objectStoreService.createBucket(bucketRequest));

        byte[] tooLargeData = new byte[2048];

        asserter.assertFailedWith(() -> objectStoreService.putObject(
                "small-limit-bucket", "too-large.bin", Multi.createFrom().item(tooLargeData),
                "application/octet-stream", null,
                null), ValidationException.class);

        asserter.execute(() -> objectStoreService.deleteBucket("small-limit-bucket"));
    }

    @Test
    @Order(15)
    @RunOnVertxContext
    void testPutObject_BucketNotFound(UniAsserter asserter) {
        asserter.assertFailedWith(() -> objectStoreService.putObject(
                "non-existent", "file.txt", Multi.createFrom().item(TEST_DATA), "text/plain", null, null),
                NotFoundException.class);
    }

    @Test
    @Order(16)
    @RunOnVertxContext
    void testPutObject_WithCustomHeaders(UniAsserter asserter) {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Custom-Header", "custom-value");
        headers.put("X-Another-Header", "another-value");

        asserter.assertThat(() -> objectStoreService.putObject(
                TEST_BUCKET, "headers-test.txt", Multi.createFrom().item(TEST_DATA), "text/plain", null, headers),
                metadata -> {
                    assertNotNull(metadata.headers);
                    assertEquals("custom-value", metadata.headers.get("X-Custom-Header"));
                });
    }

    // ==================== Object Retrieval Tests ====================

    @Test
    @Order(20)
    @RunOnVertxContext
    void testGetMetadata_Success(UniAsserter asserter) {
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, TEST_OBJECT,
                Multi.createFrom().item(TEST_DATA), "text/plain", null, null));
        asserter.assertThat(() -> objectStoreService.getMetadata(TEST_BUCKET, TEST_OBJECT), metadata -> {
            assertNotNull(metadata);
            assertEquals(TEST_OBJECT, metadata.name);
            assertEquals(TEST_DATA.length, metadata.size);
        });
    }

    @Test
    @Order(21)
    @RunOnVertxContext
    void testGetMetadata_NotFound(UniAsserter asserter) {
        asserter.assertFailedWith(() -> objectStoreService.getMetadata(TEST_BUCKET, "non-existent.txt"),
                NotFoundException.class);
    }

    @Test
    @Order(21)
    @RunOnVertxContext
    void testListObjects(UniAsserter asserter) {
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, "list-test.txt",
                Multi.createFrom().item(TEST_DATA), "text/plain", null, null));
        asserter.assertThat(() -> objectStoreService.listObjects(TEST_BUCKET), list -> {
            assertNotNull(list);
            assertTrue(list.stream().anyMatch(m -> m.name.equals("list-test.txt")));
        });
    }

    @Test
    @Order(9)
    @RunOnVertxContext
    void testGetObjectData_Success(UniAsserter asserter) {
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, TEST_OBJECT,
                Multi.createFrom().item(TEST_DATA), "text/plain", null, null));
        asserter.assertThat(() -> objectStoreService.getObjectData(TEST_BUCKET, TEST_OBJECT), retrieved -> {
            assertArrayEquals(TEST_DATA, retrieved);
        });
    }

    // ==================== Object Deletion Tests ====================

    @Test
    @Order(25)
    @RunOnVertxContext
    void testDeleteObject_Success(UniAsserter asserter) {
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, "delete-test.txt",
                Multi.createFrom().item(TEST_DATA), "text/plain", null, null));
        asserter.execute(() -> objectStoreService.deleteObject(TEST_BUCKET, "delete-test.txt"));
        asserter.assertFailedWith(() -> objectStoreService.getMetadata(TEST_BUCKET, "delete-test.txt"),
                NotFoundException.class);
    }

    // ==================== Bucket Deletion Tests ====================

    @Test
    @Order(40)
    @RunOnVertxContext
    void testDeleteBucket(UniAsserter asserter) {
        asserter.execute(() -> objectStoreService.deleteBucket(TEST_BUCKET));
        asserter.assertFailedWith(() -> objectStoreService.getBucket(TEST_BUCKET), NotFoundException.class);
    }

    // ==================== Edge Cases ====================

    @Test
    @Order(100)
    @RunOnVertxContext
    void testPutObject_EmptyFile(UniAsserter asserter) {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "empty-file-bucket";

        asserter.execute(() -> objectStoreService.createBucket(bucketRequest));

        asserter.assertThat(() -> objectStoreService.putObject(
                "empty-file-bucket", "empty.txt", Multi.createFrom().empty(), "text/plain", null, null), metadata -> {
                    assertEquals(0L, metadata.size);
                    assertEquals(0, metadata.chunkCount);
                });

        asserter.execute(() -> objectStoreService.deleteBucket("empty-file-bucket"));
    }

    @Test
    @Order(111)
    @RunOnVertxContext
    void testPutObject_SpecialCharactersInName(UniAsserter asserter) {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "special-names-bucket";

        asserter.execute(() -> objectStoreService.createBucket(bucketRequest));

        String specialName = "path/to/file with spaces (1).txt";

        asserter.assertThat(() -> objectStoreService.putObject(
                "special-names-bucket", specialName, Multi.createFrom().item(TEST_DATA), "text/plain", null, null),
                metadata -> {
                    assertEquals(specialName, metadata.name);
                });

        asserter.assertThat(() -> objectStoreService.getObjectData("special-names-bucket", specialName), data -> {
            assertArrayEquals(TEST_DATA, data);
        });

        asserter.execute(() -> objectStoreService.deleteBucket("special-names-bucket"));
    }

    @Test
    @Order(112)
    @RunOnVertxContext
    void testPutObject_BinaryData(UniAsserter asserter) {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "binary-bucket";

        asserter.execute(() -> objectStoreService.createBucket(bucketRequest));

        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }

        asserter.assertThat(() -> objectStoreService.putObject(
                "binary-bucket", "binary.bin", Multi.createFrom().item(binaryData), "application/octet-stream", null,
                null), metadata -> {
                    assertEquals(256L, metadata.size);
                });

        asserter.assertThat(() -> objectStoreService.getObjectData("binary-bucket", "binary.bin"), retrieved -> {
            assertArrayEquals(binaryData, retrieved);
        });

        asserter.execute(() -> objectStoreService.deleteBucket("binary-bucket"));
    }
    // ==================== Range Request Tests ====================

    @Test
    @Order(150)
    @RunOnVertxContext
    void testGetObjectRange_Success(UniAsserter asserter) {
        String content = "0123456789"; // 10 bytes
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, "range-test.txt",
                Multi.createFrom().item(content.getBytes()), "text/plain", null, null));

        // Test first half
        asserter.assertThat(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-test.txt", 0, 5), data -> {
            assertArrayEquals("01234".getBytes(), data);
        });

        // Test second half
        asserter.assertThat(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-test.txt", 5, 5), data -> {
            assertArrayEquals("56789".getBytes(), data);
        });

        // Test middle
        asserter.assertThat(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-test.txt", 2, 4), data -> {
            assertArrayEquals("2345".getBytes(), data);
        });

        // Test beyond end (should clamp)
        asserter.assertThat(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-test.txt", 8, 10), data -> {
            assertArrayEquals("89".getBytes(), data);
        });
    }

    @Test
    @Order(151)
    @RunOnVertxContext
    void testGetObjectRange_Invalid(UniAsserter asserter) {
        String content = "test";
        asserter.execute(() -> objectStoreService.putObject(
                TEST_BUCKET, "range-invalid.txt",
                Multi.createFrom().item(content.getBytes()), "text/plain", null, null));

        // Negative offset
        asserter.assertFailedWith(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-invalid.txt", -1, 5),
                ValidationException.class);

        // Offset beyond size
        asserter.assertFailedWith(() -> objectStoreService.getObjectRange(TEST_BUCKET, "range-invalid.txt", 10, 5),
                ValidationException.class);
    }

    @Test
    @Order(152)
    @RunOnVertxContext
    void testUpload5MiB_Object_ChunksAndStreaming(UniAsserter asserter) {
        // Note: 5 MiB is enough to verify chunking and reactive streaming
        // implementation.
        byte[] huge = new byte[5 * 1024 * 1024]; // 5 MiB
        new Random().nextBytes(huge);

        asserter.execute(() -> objectStoreService
                .putObject(TEST_BUCKET, "huge.bin", Multi.createFrom().item(huge), "application/octet-stream", null,
                        null)
                .onItem().invoke(meta -> {
                    assertEquals(5L * 1024 * 1024, meta.size);
                    assertTrue(meta.chunkCount >= 5, "Should have 5 chunks of 1 MiB");
                })
                .chain(() -> objectStoreService.getObjectData(TEST_BUCKET, "huge.bin"))
                .onItem().invoke(downloaded -> {
                    assertEquals(huge.length, downloaded.length);
                    assertArrayEquals(huge, downloaded);
                }));
    }
}
