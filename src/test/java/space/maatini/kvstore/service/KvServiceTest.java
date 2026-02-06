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
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.util.Base64;
import java.util.List;
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
    void setUp() {
        // Reset mock
        Mockito.reset(watchService);
    }

    // ==================== Bucket Tests ====================

    @Test
    @Order(1)
    void testCreateBucket_Success() {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = TEST_BUCKET;
        request.description = "Test bucket for unit tests";
        request.maxValueSize = 1024;
        request.maxHistoryPerKey = 5;

        KvBucket bucket = kvService.createBucket(request);

        assertNotNull(bucket);
        assertNotNull(bucket.id);
        assertEquals(TEST_BUCKET, bucket.name);
        assertEquals("Test bucket for unit tests", bucket.description);
        assertEquals(1024, bucket.maxValueSize);
        assertEquals(5, bucket.maxHistoryPerKey);
        assertNotNull(bucket.createdAt);

        testBucketId = bucket.id;
    }

    @Test
    @Order(2)
    void testCreateBucket_DuplicateName() {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = TEST_BUCKET;

        assertThrows(ConflictException.class, () -> kvService.createBucket(request));
    }

    @Test
    @Order(3)
    void testGetBucket_Success() {
        KvBucket bucket = kvService.getBucket(TEST_BUCKET);

        assertNotNull(bucket);
        assertEquals(TEST_BUCKET, bucket.name);
    }

    @Test
    @Order(4)
    void testGetBucket_NotFound() {
        assertThrows(NotFoundException.class, () -> kvService.getBucket("non-existent"));
    }

    @Test
    @Order(5)
    void testListBuckets() {
        List<KvBucket> buckets = kvService.listBuckets();

        assertNotNull(buckets);
        assertTrue(buckets.stream().anyMatch(b -> b.name.equals(TEST_BUCKET)));
    }

    @Test
    @Order(6)
    void testUpdateBucket() {
        KvBucketDto.UpdateRequest request = new KvBucketDto.UpdateRequest();
        request.description = "Updated description";
        request.maxValueSize = 2048;

        KvBucket bucket = kvService.updateBucket(TEST_BUCKET, request);

        assertNotNull(bucket);
        assertEquals("Updated description", bucket.description);
        assertEquals(2048, bucket.maxValueSize);
    }

    // ==================== Key-Value Entry Tests ====================

    @Test
    @Order(10)
    void testPut_Success() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Hello, World!";
        request.base64 = false;

        KvEntry entry = kvService.put(TEST_BUCKET, "test-key", request);

        assertNotNull(entry);
        assertEquals("test-key", entry.key);
        assertEquals(1L, entry.revision);
        assertEquals(KvEntry.Operation.PUT, entry.operation);
        assertArrayEquals("Hello, World!".getBytes(), entry.value);

        // Verify watcher was notified
        verify(watchService, times(1)).notifyChange(any(KvEntryDto.WatchEvent.class));
    }

    @Test
    @Order(11)
    void testPut_Base64Value() {
        String originalValue = "Binary data: \u0000\u0001\u0002";
        String base64Value = Base64.getEncoder().encodeToString(originalValue.getBytes());

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = base64Value;
        request.base64 = true;

        KvEntry entry = kvService.put(TEST_BUCKET, "binary-key", request);

        assertNotNull(entry);
        assertArrayEquals(originalValue.getBytes(), entry.value);
    }

    @Test
    @Order(12)
    void testPut_InvalidBase64() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "not-valid-base64!!!";
        request.base64 = true;

        assertThrows(ValidationException.class, () -> kvService.put(TEST_BUCKET, "invalid-key", request));
    }

    @Test
    @Order(13)
    void testPut_ValueTooLarge() {
        // Create a large value (bucket has maxValueSize=2048 from update test)
        byte[] largeValue = new byte[3000];
        String base64Value = Base64.getEncoder().encodeToString(largeValue);

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = base64Value;
        request.base64 = true;

        assertThrows(ValidationException.class, () -> kvService.put(TEST_BUCKET, "large-key", request));
    }

    @Test
    @Order(14)
    void testPut_WithTTL() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Expiring value";
        request.base64 = false;
        request.ttlSeconds = 3600L;

        KvEntry entry = kvService.put(TEST_BUCKET, "ttl-key", request);

        assertNotNull(entry);
        assertNotNull(entry.expiresAt);
        assertTrue(entry.expiresAt.isAfter(entry.createdAt));
    }

    @Test
    @Order(15)
    void testPut_BucketNotFound() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;

        assertThrows(NotFoundException.class, () -> kvService.put("non-existent-bucket", "key", request));
    }

    @Test
    @Order(20)
    void testGet_Success() {
        KvEntry entry = kvService.get(TEST_BUCKET, "test-key");

        assertNotNull(entry);
        assertEquals("test-key", entry.key);
        assertArrayEquals("Hello, World!".getBytes(), entry.value);
    }

    @Test
    @Order(21)
    void testGet_NotFound() {
        assertThrows(NotFoundException.class, () -> kvService.get(TEST_BUCKET, "non-existent-key"));
    }

    @Test
    @Order(22)
    void testPut_CreatesNewRevision() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Updated value";
        request.base64 = false;

        KvEntry entry = kvService.put(TEST_BUCKET, "test-key", request);

        assertNotNull(entry);
        assertEquals(2L, entry.revision);
    }

    @Test
    @Order(23)
    void testGetRevision_Success() {
        KvEntry entry = kvService.getRevision(TEST_BUCKET, "test-key", 1L);

        assertNotNull(entry);
        assertEquals(1L, entry.revision);
        assertArrayEquals("Hello, World!".getBytes(), entry.value);
    }

    @Test
    @Order(24)
    void testGetRevision_NotFound() {
        assertThrows(NotFoundException.class, () -> kvService.getRevision(TEST_BUCKET, "test-key", 999L));
    }

    @Test
    @Order(25)
    void testGetHistory() {
        List<KvEntry> history = kvService.getHistory(TEST_BUCKET, "test-key", 10);

        assertNotNull(history);
        assertEquals(2, history.size());
        // History should be ordered by revision DESC
        assertEquals(2L, history.get(0).revision);
        assertEquals(1L, history.get(1).revision);
    }

    @Test
    @Order(26)
    void testGetHistory_WithLimit() {
        List<KvEntry> history = kvService.getHistory(TEST_BUCKET, "test-key", 1);

        assertNotNull(history);
        assertEquals(1, history.size());
        assertEquals(2L, history.get(0).revision);
    }

    @Test
    @Order(27)
    void testListKeys() {
        List<String> keys = kvService.listKeys(TEST_BUCKET);

        assertNotNull(keys);
        assertTrue(keys.contains("test-key"));
        assertTrue(keys.contains("binary-key"));
        assertTrue(keys.contains("ttl-key"));
    }

    @Test
    @Order(30)
    void testDelete_Success() {
        KvEntry entry = kvService.delete(TEST_BUCKET, "test-key");

        assertNotNull(entry);
        assertEquals(KvEntry.Operation.DELETE, entry.operation);
        assertEquals(3L, entry.revision);
        assertNull(entry.value);

        // Verify watcher was notified
        verify(watchService, times(1)).notifyChange(any(KvEntryDto.WatchEvent.class));
    }

    @Test
    @Order(31)
    void testGet_AfterDelete() {
        // Trying to get a deleted key should throw NotFoundException
        assertThrows(NotFoundException.class, () -> kvService.get(TEST_BUCKET, "test-key"));
    }

    @Test
    @Order(32)
    void testDelete_NotFound() {
        assertThrows(NotFoundException.class, () -> kvService.delete(TEST_BUCKET, "non-existent-key"));
    }

    @Test
    @Order(40)
    void testPurgeKey() {
        // Add a key with multiple revisions
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "v1";
        request.base64 = false;
        kvService.put(TEST_BUCKET, "purge-key", request);

        request.value = "v2";
        kvService.put(TEST_BUCKET, "purge-key", request);

        request.value = "v3";
        kvService.put(TEST_BUCKET, "purge-key", request);

        // Purge the key (permanently delete all revisions)
        long deleted = kvService.purge(TEST_BUCKET, "purge-key");

        assertEquals(3, deleted);

        // Key should no longer exist
        assertThrows(NotFoundException.class, () -> kvService.get(TEST_BUCKET, "purge-key"));
    }

    @Test
    @Order(41)
    void testPurgeBucket() {
        // Add some keys
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;

        kvService.put(TEST_BUCKET, "purge-bucket-key-1", request);
        kvService.put(TEST_BUCKET, "purge-bucket-key-2", request);

        // Purge the bucket
        long deleted = kvService.purgeBucket(TEST_BUCKET);

        assertTrue(deleted > 0);

        // Keys should be empty
        List<String> keys = kvService.listKeys(TEST_BUCKET);
        assertFalse(keys.contains("purge-bucket-key-1"));
        assertFalse(keys.contains("purge-bucket-key-2"));
    }

    @Test
    @Order(100)
    void testDeleteBucket() {
        kvService.deleteBucket(TEST_BUCKET);

        assertThrows(NotFoundException.class, () -> kvService.getBucket(TEST_BUCKET));
    }

    // ==================== Edge Cases ====================

    @Test
    @Order(110)
    void testPut_EmptyValue() {
        // Create new bucket for this test
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "empty-value-bucket";
        kvService.createBucket(bucketRequest);

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "";
        request.base64 = false;

        KvEntry entry = kvService.put("empty-value-bucket", "empty-key", request);

        assertNotNull(entry);
        assertArrayEquals(new byte[0], entry.value);

        // Cleanup
        kvService.deleteBucket("empty-value-bucket");
    }

    @Test
    @Order(111)
    void testPut_NullValue() {
        // Create new bucket for this test
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "null-value-bucket";
        kvService.createBucket(bucketRequest);

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = null;
        request.base64 = false;

        KvEntry entry = kvService.put("null-value-bucket", "null-key", request);

        assertNotNull(entry);
        assertArrayEquals(new byte[0], entry.value);

        // Cleanup
        kvService.deleteBucket("null-value-bucket");
    }

    @Test
    @Order(112)
    void testPut_SpecialCharactersInKey() {
        KvBucketDto.CreateRequest bucketRequest = new KvBucketDto.CreateRequest();
        bucketRequest.name = "special-chars-bucket";
        kvService.createBucket(bucketRequest);

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;

        String specialKey = "key/with/slashes.and-dashes_underscores:colons";
        KvEntry entry = kvService.put("special-chars-bucket", specialKey, request);

        assertNotNull(entry);
        assertEquals(specialKey, entry.key);

        // Verify retrieval
        KvEntry retrieved = kvService.get("special-chars-bucket", specialKey);
        assertEquals(specialKey, retrieved.key);

        // Cleanup
        kvService.deleteBucket("special-chars-bucket");
    }
}
