package space.maatini.kvstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.kvstore.dto.KvBucketDto;
import space.maatini.kvstore.dto.KvEntryDto;
import space.maatini.kvstore.entity.KvBucket;
import space.maatini.kvstore.entity.KvEntry;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.hibernate.reactive.panache.TransactionalUniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KvService.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KvServiceTest {

    @Inject
    KvService kvService;

    @InjectMock
    KvWatchService watchService;

    // Test bucket for operations
    private static final String TEST_BUCKET = "unit-test-bucket";
    private static UUID testBucketId;

    @BeforeEach
    void setUp(TransactionalUniAsserter asserter) {
        Mockito.reset(watchService);
        doReturn(io.smallrye.mutiny.Uni.createFrom().voidItem())
                .when(watchService).notifyChange(any());
        asserter.execute(() -> KvEntry.deleteAll());
        asserter.execute(() -> KvBucket.deleteAll());
        asserter.execute(() -> {
            KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
            request.name = TEST_BUCKET;
            request.maxValueSize = 1024;
            request.maxHistoryPerKey = 5;
            return kvService.createBucket(request)
                    .invoke(bucket -> testBucketId = bucket.id)
                    .replaceWithVoid();
        });
    }

    // ==================== Bucket Tests ====================

    @Test
    @Order(1)
    @RunOnVertxContext
    void testCreateBucket_Success(TransactionalUniAsserter asserter) {
        String newBucket = "new-bucket";
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = newBucket;
        request.description = "Test bucket for unit tests";
        request.maxValueSize = 1024;
        request.maxHistoryPerKey = 5;

        asserter.assertThat(() -> kvService.createBucket(request), bucket -> {
            assertNotNull(bucket);
            assertNotNull(bucket.id);
            assertEquals(newBucket, bucket.name);
            assertEquals("Test bucket for unit tests", bucket.description);
            assertEquals(1024, bucket.maxValueSize);
            assertEquals(5, bucket.maxHistoryPerKey);
            assertNotNull(bucket.createdAt);
        });
    }

    @Test
    @Order(2)
    @RunOnVertxContext
    void testCreateBucket_DuplicateName(TransactionalUniAsserter asserter) {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = TEST_BUCKET;
        request.description = "Duplicate bucket";

        asserter.assertFailedWith(() -> kvService.createBucket(request), ConflictException.class);
    }

    @Test
    @Order(3)
    @RunOnVertxContext
    void testGetBucket_Success(TransactionalUniAsserter asserter) {
        asserter.assertThat(() -> kvService.getBucket(TEST_BUCKET), bucket -> {
            assertNotNull(bucket);
            assertEquals(TEST_BUCKET, bucket.name);
            assertEquals(testBucketId, bucket.id);
        });
    }

    @Test
    @Order(4)
    @RunOnVertxContext
    void testGetBucket_NotFound(TransactionalUniAsserter asserter) {
        asserter.assertFailedWith(() -> kvService.getBucket("nonexistent"), NotFoundException.class);
    }

    @Test
    @Order(5)
    @RunOnVertxContext
    void testListBuckets(TransactionalUniAsserter asserter) {
        asserter.assertThat(() -> kvService.listBuckets(), buckets -> {
            assertNotNull(buckets);
            assertFalse(buckets.isEmpty());
            assertTrue(buckets.stream().anyMatch(b -> b.name.equals(TEST_BUCKET)));
        });
    }

    @Test
    @Order(6)
    @RunOnVertxContext
    void testUpdateBucket(TransactionalUniAsserter asserter) {
        KvBucketDto.UpdateRequest request = new KvBucketDto.UpdateRequest();
        request.description = "Updated description";
        request.maxValueSize = 2048;
        request.maxHistoryPerKey = 10;

        asserter.assertThat(() -> kvService.updateBucket(TEST_BUCKET, request), bucket -> {
            assertNotNull(bucket);
            assertEquals("Updated description", bucket.description);
            assertEquals(2048, bucket.maxValueSize);
            assertEquals(10, bucket.maxHistoryPerKey);
        });
    }

    // ==================== Key-Value Entry Tests ====================

    @Test
    @Order(7)
    @RunOnVertxContext
    void testPut_Success(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "test-value";
        request.base64 = false;

        asserter.assertThat(() -> kvService.put(TEST_BUCKET, "test-key", request), entry -> {
            assertNotNull(entry);
            assertEquals("test-key", entry.key);
            assertArrayEquals("test-value".getBytes(), entry.value);
            assertEquals(1L, entry.revision);
        });
    }

    @Test
    @Order(8)
    @RunOnVertxContext
    void testPut_Base64Value(TransactionalUniAsserter asserter) {
        String originalValue = "Hello, World!";
        String base64Value = Base64.getEncoder().encodeToString(originalValue.getBytes());

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = base64Value;
        request.base64 = true;

        asserter.assertThat(() -> kvService.put(TEST_BUCKET, "base64-key", request), entry -> {
            assertNotNull(entry);
            assertEquals("base64-key", entry.key);
            assertArrayEquals(originalValue.getBytes(), entry.value);
        });
    }

    @Test
    @Order(9)
    @RunOnVertxContext
    void testPut_InvalidBase64(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "invalid-base64!";
        request.base64 = true;

        asserter.assertFailedWith(() -> kvService.put(TEST_BUCKET, "invalid-key", request), ValidationException.class);
    }

    @Test
    @Order(10)
    @RunOnVertxContext
    void testPut_ValueTooLarge(TransactionalUniAsserter asserter) {
        // Create a large value (bucket has maxValueSize=2048 from update test)
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "A".repeat(2000); // Exceeds 1024
        request.base64 = false;

        asserter.assertFailedWith(() -> kvService.put(TEST_BUCKET, "large-key", request), ValidationException.class);
    }

    @Test
    @Order(11)
    @RunOnVertxContext
    void testPut_BucketNotFound(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";

        asserter.assertFailedWith(() -> kvService.put("nonexistent", "key", request), NotFoundException.class);
    }

    @Test
    @Order(12)
    @RunOnVertxContext
    void testGet_Success(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "test-value";
        asserter.execute(() -> kvService.put(TEST_BUCKET, "test-key", request));
        asserter.assertThat(() -> kvService.get(TEST_BUCKET, "test-key"), entry -> {
            assertNotNull(entry);
            assertEquals("test-key", entry.key);
            assertArrayEquals("test-value".getBytes(), entry.value);
        });
    }

    @Test
    @Order(13)
    @RunOnVertxContext
    void testGet_NotFound(TransactionalUniAsserter asserter) {
        asserter.assertFailedWith(() -> kvService.get(TEST_BUCKET, "nonexistent"), NotFoundException.class);
    }

    @Test
    @Order(14)
    @RunOnVertxContext
    void testPut_CreatesNewRevision(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request1 = new KvEntryDto.PutRequest();
        request1.value = "revision-1";

        KvEntryDto.PutRequest request2 = new KvEntryDto.PutRequest();
        request2.value = "revision-2";

        asserter.execute(() -> kvService.put(TEST_BUCKET, "test-key", request1));
        asserter.assertThat(() -> kvService.put(TEST_BUCKET, "test-key", request2), entry -> {
            assertNotNull(entry);
            assertEquals(2L, entry.revision);
            assertArrayEquals("revision-2".getBytes(), entry.value);
        });
    }

    @Test
    @Order(15)
    @RunOnVertxContext
    void testPut_WithTTL(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Expiring value";
        request.base64 = false;
        request.ttlSeconds = 3600L;

        asserter.assertThat(() -> kvService.put(TEST_BUCKET, "ttl-key", request), entry -> {
            assertNotNull(entry);
            assertNotNull(entry.expiresAt);
            assertTrue(entry.expiresAt.isAfter(entry.createdAt));
        });
    }

    @Test
    @Order(23)
    @RunOnVertxContext
    void testGetRevision_Success(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "test-value";
        asserter.execute(() -> kvService.put(TEST_BUCKET, "test-key", request));
        asserter.assertThat(() -> kvService.getRevision(TEST_BUCKET, "test-key", 1L), entry -> {
            assertNotNull(entry);
            assertEquals(1L, entry.revision);
            assertArrayEquals("test-value".getBytes(), entry.value);
        });
    }

    @Test
    @Order(24)
    @RunOnVertxContext
    void testGetRevision_NotFound(TransactionalUniAsserter asserter) {
        asserter.assertFailedWith(() -> kvService.getRevision(TEST_BUCKET, "test-key", 999L), NotFoundException.class);
    }

    @Test
    @Order(25)
    @RunOnVertxContext
    void testGetHistory(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request1 = new KvEntryDto.PutRequest();
        request1.value = "v1";
        KvEntryDto.PutRequest request2 = new KvEntryDto.PutRequest();
        request2.value = "v2";

        asserter.execute(() -> kvService.put(TEST_BUCKET, "hist-key", request1));
        asserter.execute(() -> kvService.put(TEST_BUCKET, "hist-key", request2));

        asserter.assertThat(() -> kvService.getHistory(TEST_BUCKET, "hist-key", 10), history -> {
            assertNotNull(history);
            assertEquals(2, history.size());
            assertEquals(2L, history.get(0).revision);
            assertEquals(1L, history.get(1).revision);
        });
    }

    @Test
    @Order(26)
    @RunOnVertxContext
    void testGetHistory_WithLimit(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request1 = new KvEntryDto.PutRequest();
        request1.value = "v1";
        KvEntryDto.PutRequest request2 = new KvEntryDto.PutRequest();
        request2.value = "v2";

        asserter.execute(() -> kvService.put(TEST_BUCKET, "hist-limit-key", request1));
        asserter.execute(() -> kvService.put(TEST_BUCKET, "hist-limit-key", request2));

        asserter.assertThat(() -> kvService.getHistory(TEST_BUCKET, "hist-limit-key", 1), history -> {
            assertNotNull(history);
            assertEquals(1, history.size());
            assertEquals(2L, history.get(0).revision);
        });
    }

    @Test
    @Order(27)
    @RunOnVertxContext
    void testListKeys(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "val";

        asserter.execute(() -> kvService.put(TEST_BUCKET, "list-key-1", request));
        asserter.execute(() -> kvService.put(TEST_BUCKET, "list-key-2", request));

        asserter.assertThat(() -> kvService.listKeys(TEST_BUCKET), keys -> {
            assertNotNull(keys);
            assertTrue(keys.contains("list-key-1"));
            assertTrue(keys.contains("list-key-2"));
        });
    }

    @Test
    @Order(30)
    @RunOnVertxContext
    void testDelete_Success(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "val";
        asserter.execute(() -> kvService.put(TEST_BUCKET, "delete-key", request));

        asserter.assertThat(() -> kvService.delete(TEST_BUCKET, "delete-key"), entry -> {
            assertNotNull(entry);
            assertEquals(KvEntry.Operation.DELETE, entry.operation);
            assertEquals(2L, entry.revision);
            assertNull(entry.value);
        });
        asserter.execute(() -> verify(watchService, atLeastOnce()).notifyChange(any(KvEntryDto.WatchEvent.class)));
    }

    @Test
    @Order(31)
    @RunOnVertxContext
    void testGet_AfterDelete(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "val";
        asserter.execute(() -> kvService.put(TEST_BUCKET, "delete-get-key", request));
        asserter.execute(() -> kvService.delete(TEST_BUCKET, "delete-get-key"));

        asserter.assertFailedWith(() -> kvService.get(TEST_BUCKET, "delete-get-key"), NotFoundException.class);
    }

    @Test
    @Order(32)
    @RunOnVertxContext
    void testDelete_NotFound(TransactionalUniAsserter asserter) {
        asserter.assertFailedWith(() -> kvService.delete(TEST_BUCKET, "non-existent-key"), NotFoundException.class);
    }

    @Test
    @Order(40)
    @RunOnVertxContext
    void testPurgeKey(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "v1";
        request.base64 = false;

        asserter.execute(() -> kvService.put(TEST_BUCKET, "purge-key", request));
        asserter.execute(() -> {
            request.value = "v2";
            return kvService.put(TEST_BUCKET, "purge-key", request);
        });
        asserter.execute(() -> {
            request.value = "v3";
            return kvService.put(TEST_BUCKET, "purge-key", request);
        });

        asserter.assertThat(() -> kvService.purge(TEST_BUCKET, "purge-key"), deleted -> {
            assertEquals(3, deleted);
        });

        asserter.assertFailedWith(() -> kvService.get(TEST_BUCKET, "purge-key"), NotFoundException.class);
    }

    @Test
    @Order(41)
    @RunOnVertxContext
    void testPurgeBucket(TransactionalUniAsserter asserter) {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;

        asserter.execute(() -> kvService.put(TEST_BUCKET, "purge-bucket-key-1", request));
        asserter.execute(() -> kvService.put(TEST_BUCKET, "purge-bucket-key-2", request));

        asserter.assertThat(() -> kvService.purgeBucket(TEST_BUCKET), deleted -> {
            assertTrue(deleted > 0);
        });

        asserter.assertThat(() -> kvService.listKeys(TEST_BUCKET), keys -> {
            assertFalse(keys.contains("purge-bucket-key-1"));
            assertFalse(keys.contains("purge-bucket-key-2"));
        });
    }

    @Test
    @Order(100)
    @RunOnVertxContext
    void testDeleteBucket(TransactionalUniAsserter asserter) {
        asserter.execute(() -> kvService.deleteBucket(TEST_BUCKET));
        asserter.assertFailedWith(() -> kvService.getBucket(TEST_BUCKET), NotFoundException.class);
    }

    // ==================== Edge Cases ====================

    @Test
    @Order(110)
    @RunOnVertxContext
    void testPut_EmptyValue(TransactionalUniAsserter asserter) {
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "empty-value-bucket";

        asserter.execute(() -> kvService.createBucket(bucketRequest));

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "";
        request.base64 = false;

        asserter.assertThat(() -> kvService.put("empty-value-bucket", "empty-key", request), entry -> {
            assertNotNull(entry);
            assertArrayEquals(new byte[0], entry.value);
        });

        asserter.execute(() -> kvService.deleteBucket("empty-value-bucket"));
    }

    @Test
    @Order(111)
    @RunOnVertxContext
    void testPut_NullValue(TransactionalUniAsserter asserter) {
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "null-value-bucket";

        asserter.execute(() -> kvService.createBucket(bucketRequest));

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = null;
        request.base64 = false;

        asserter.assertThat(() -> kvService.put("null-value-bucket", "null-key", request), entry -> {
            assertNotNull(entry);
            assertArrayEquals(new byte[0], entry.value);
        });

        asserter.execute(() -> kvService.deleteBucket("null-value-bucket"));
    }

    @Test
    @Order(112)
    @RunOnVertxContext
    void testPut_SpecialCharactersInKey(TransactionalUniAsserter asserter) {
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "special-chars-bucket";

        asserter.execute(() -> kvService.createBucket(bucketRequest));

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;

        String specialKey = "key/with/slashes.and-dashes_underscores:colons";

        asserter.assertThat(() -> kvService.put("special-chars-bucket", specialKey, request), entry -> {
            assertNotNull(entry);
            assertEquals(specialKey, entry.key);
        });

        asserter.assertThat(() -> kvService.get("special-chars-bucket", specialKey), retrieved -> {
            assertEquals(specialKey, retrieved.key);
        });

        asserter.execute(() -> kvService.deleteBucket("special-chars-bucket"));
    }
}
