package space.maatini.kvstore.entity;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KV entities.
 */
class KvEntityTest {

    // ==================== KvBucket Tests ====================

    @Test
    void testKvBucket_Creation() {
        KvBucket bucket = new KvBucket();
        bucket.id = UUID.randomUUID();
        bucket.name = "test-bucket";
        bucket.description = "Test description";
        bucket.maxValueSize = 1024;
        bucket.maxHistoryPerKey = 10;
        bucket.createdAt = OffsetDateTime.now();

        assertNotNull(bucket.id);
        assertEquals("test-bucket", bucket.name);
        assertEquals("Test description", bucket.description);
        assertEquals(1024, bucket.maxValueSize);
        assertEquals(10, bucket.maxHistoryPerKey);
        assertNotNull(bucket.createdAt);
    }

    @Test
    void testKvBucket_DefaultValues() {
        KvBucket bucket = new KvBucket();

        // Fields without defaults
        assertNull(bucket.id);
        assertNull(bucket.name);
        assertNull(bucket.description);
        assertNull(bucket.createdAt);
        assertNull(bucket.ttlSeconds);

        // Fields with defaults
        assertEquals(1048576, bucket.maxValueSize); // 1MB default
        assertEquals(100, bucket.maxHistoryPerKey); // 100 default
    }

    // ==================== KvEntry Tests ====================

    @Test
    void testKvEntry_Creation() {
        KvEntry entry = new KvEntry();
        entry.id = UUID.randomUUID();
        entry.key = "my-key";
        entry.value = "my-value".getBytes();
        entry.revision = 1L;
        entry.operation = KvEntry.Operation.PUT;
        entry.createdAt = OffsetDateTime.now();

        assertNotNull(entry.id);
        assertEquals("my-key", entry.key);
        assertArrayEquals("my-value".getBytes(), entry.value);
        assertEquals(1L, entry.revision);
        assertEquals(KvEntry.Operation.PUT, entry.operation);
        assertNotNull(entry.createdAt);
    }

    @Test
    void testKvEntry_Operations() {
        assertEquals("PUT", KvEntry.Operation.PUT.name());
        assertEquals("DELETE", KvEntry.Operation.DELETE.name());
        assertEquals("PURGE", KvEntry.Operation.PURGE.name());
    }

    @Test
    void testKvEntry_DeleteOperation() {
        KvEntry entry = new KvEntry();
        entry.key = "deleted-key";
        entry.operation = KvEntry.Operation.DELETE;
        entry.value = null; // Deleted entries have no value

        assertEquals(KvEntry.Operation.DELETE, entry.operation);
        assertNull(entry.value);
    }

    @Test
    void testKvEntry_WithBucketId() {
        UUID bucketId = UUID.randomUUID();

        KvEntry entry = new KvEntry();
        entry.bucketId = bucketId;
        entry.key = "my-key";

        assertEquals(bucketId, entry.bucketId);
    }

    @Test
    void testKvEntry_WithTTL() {
        KvEntry entry = new KvEntry();
        entry.key = "expiring-key";
        entry.createdAt = OffsetDateTime.now();
        entry.expiresAt = OffsetDateTime.now().plusHours(1);

        assertNotNull(entry.expiresAt);
        assertTrue(entry.expiresAt.isAfter(entry.createdAt));
    }

    @Test
    void testKvEntry_BinaryValue() {
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }

        KvEntry entry = new KvEntry();
        entry.value = binaryData;

        assertEquals(256, entry.value.length);
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, entry.value[i]);
        }
    }

    @Test
    void testKvEntry_EmptyValue() {
        KvEntry entry = new KvEntry();
        entry.value = new byte[0];

        assertNotNull(entry.value);
        assertEquals(0, entry.value.length);
    }

    @Test
    void testKvEntry_MultipleRevisions() {
        KvEntry entry1 = new KvEntry();
        entry1.key = "same-key";
        entry1.revision = 1L;
        entry1.value = "v1".getBytes();

        KvEntry entry2 = new KvEntry();
        entry2.key = "same-key";
        entry2.revision = 2L;
        entry2.value = "v2".getBytes();

        KvEntry entry3 = new KvEntry();
        entry3.key = "same-key";
        entry3.revision = 3L;
        entry3.value = "v3".getBytes();

        assertEquals("same-key", entry1.key);
        assertEquals("same-key", entry2.key);
        assertEquals("same-key", entry3.key);
        assertTrue(entry3.revision > entry2.revision);
        assertTrue(entry2.revision > entry1.revision);
    }

    @Test
    void testKvEntry_DefaultRevision() {
        KvEntry entry = new KvEntry();

        assertEquals(1L, entry.revision); // Default value
    }

    @Test
    void testKvEntry_DefaultOperation() {
        KvEntry entry = new KvEntry();

        assertEquals(KvEntry.Operation.PUT, entry.operation); // Default value
    }
}
