package space.maatini.kvstore.dto;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KV DTOs.
 */
class KvDtoTest {

    // ==================== KvBucketDto Tests ====================

    @Test
    void testCreateRequest_DefaultValues() {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();

        assertNull(request.name);
        assertNull(request.description);
        assertNull(request.maxValueSize);
        assertNull(request.maxHistoryPerKey);
    }

    @Test
    void testResponse_FromBucket() {
        UUID id = UUID.randomUUID();
        String name = "test-bucket";
        String description = "Test description";
        int maxValueSize = 1024;
        int maxHistoryPerKey = 10;
        OffsetDateTime createdAt = OffsetDateTime.now();

        KvBucketDto.Response response = new KvBucketDto.Response();
        response.id = id;
        response.name = name;
        response.description = description;
        response.maxValueSize = maxValueSize;
        response.maxHistoryPerKey = maxHistoryPerKey;
        response.createdAt = createdAt;

        assertEquals(id, response.id);
        assertEquals(name, response.name);
        assertEquals(description, response.description);
        assertEquals(maxValueSize, response.maxValueSize);
        assertEquals(maxHistoryPerKey, response.maxHistoryPerKey);
        assertEquals(createdAt, response.createdAt);
    }

    // ==================== KvEntryDto Tests ====================

    @Test
    void testPutRequest_TextValue() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Hello, World!";
        request.base64 = false;

        assertEquals("Hello, World!", request.value);
        assertFalse(request.base64);
    }

    @Test
    void testPutRequest_Base64Value() {
        String original = "Binary data";
        String encoded = Base64.getEncoder().encodeToString(original.getBytes());

        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = encoded;
        request.base64 = true;

        assertTrue(request.base64);

        byte[] decoded = Base64.getDecoder().decode(request.value);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testPutRequest_WithTTL() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "value";
        request.base64 = false;
        request.ttlSeconds = 3600L;

        assertEquals(3600L, request.ttlSeconds);
    }

    @Test
    void testResponse_Fields() {
        KvEntryDto.Response response = new KvEntryDto.Response();
        response.key = "test-key";
        response.value = "dGVzdA=="; // base64 for "test"
        response.revision = 1L;
        response.operation = "PUT";
        response.createdAt = OffsetDateTime.now();
        response.expiresAt = null;

        assertEquals("test-key", response.key);
        assertEquals("dGVzdA==", response.value);
        assertEquals(1L, response.revision);
        assertEquals("PUT", response.operation);
        assertNotNull(response.createdAt);
        assertNull(response.expiresAt);
    }

    @Test
    void testWatchEvent_Creation() {
        KvEntryDto.WatchEvent event = new KvEntryDto.WatchEvent();
        event.type = "PUT";
        event.bucket = "test-bucket";
        event.key = "test-key";
        event.value = "dGVzdA==";
        event.revision = 5L;
        event.timestamp = OffsetDateTime.now();

        assertEquals("PUT", event.type);
        assertEquals("test-bucket", event.bucket);
        assertEquals("test-key", event.key);
        assertEquals("dGVzdA==", event.value);
        assertEquals(5L, event.revision);
        assertNotNull(event.timestamp);
    }

    @Test
    void testWatchEvent_DeleteOperation() {
        KvEntryDto.WatchEvent event = new KvEntryDto.WatchEvent();
        event.type = "DELETE";
        event.bucket = "test-bucket";
        event.key = "deleted-key";
        event.value = null;
        event.revision = 10L;

        assertEquals("DELETE", event.type);
        assertNull(event.value);
    }

    @Test
    void testKeyInfo_Creation() {
        KvEntryDto.KeyInfo info = new KvEntryDto.KeyInfo();
        info.key = "my-key";
        info.revision = 3L;
        info.operation = "PUT";
        info.createdAt = OffsetDateTime.now();

        assertEquals("my-key", info.key);
        assertEquals(3L, info.revision);
        assertEquals("PUT", info.operation);
        assertNotNull(info.createdAt);
    }
}
