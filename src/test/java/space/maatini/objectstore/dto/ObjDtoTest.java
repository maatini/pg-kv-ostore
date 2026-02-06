package space.maatini.objectstore.dto;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Object Store DTOs.
 */
class ObjDtoTest {

    // ==================== ObjBucketDto Tests ====================

    @Test
    void testCreateRequest_DefaultValues() {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();

        assertNull(request.name);
        assertNull(request.description);
        assertNull(request.chunkSize);
        assertNull(request.maxObjectSize);
    }

    @Test
    void testCreateRequest_WithValues() {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = "my-bucket";
        request.description = "My bucket description";
        request.chunkSize = 1024 * 1024; // 1MB
        request.maxObjectSize = 1024L * 1024L * 1024L; // 1GB

        assertEquals("my-bucket", request.name);
        assertEquals("My bucket description", request.description);
        assertEquals(1024 * 1024, request.chunkSize);
        assertEquals(1024L * 1024L * 1024L, request.maxObjectSize);
    }

    @Test
    void testBucketResponse() {
        ObjBucketDto.Response response = new ObjBucketDto.Response();
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        response.id = id;
        response.name = "test-bucket";
        response.description = "Test description";
        response.chunkSize = 1048576;
        response.maxObjectSize = 1073741824L;
        response.createdAt = now;

        assertEquals(id, response.id);
        assertEquals("test-bucket", response.name);
        assertEquals("Test description", response.description);
        assertEquals(1048576, response.chunkSize);
        assertEquals(1073741824L, response.maxObjectSize);
        assertEquals(now, response.createdAt);
    }

    // ==================== ObjMetadataDto Tests ====================

    @Test
    void testObjectInfo_BasicProperties() {
        ObjMetadataDto.ObjectInfo info = new ObjMetadataDto.ObjectInfo();

        info.name = "document.pdf";
        info.size = 1024000L;
        info.contentType = "application/pdf";
        info.createdAt = OffsetDateTime.now();

        assertEquals("document.pdf", info.name);
        assertEquals(1024000L, info.size);
        assertEquals("application/pdf", info.contentType);
        assertNotNull(info.createdAt);
    }

    @Test
    void testMetadataResponse_FullObject() {
        ObjMetadataDto.Response response = new ObjMetadataDto.Response();
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        response.id = id;
        response.bucket = "files-bucket";
        response.name = "image.png";
        response.size = 500000L;
        response.contentType = "image/png";
        response.description = "Profile picture";
        response.digest = "abc123def456";
        response.digestAlgorithm = "SHA-256";
        response.chunkCount = 1;
        response.createdAt = now;
        response.updatedAt = now;

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Custom-Header", "value");
        response.headers = headers;

        assertEquals(id, response.id);
        assertEquals("files-bucket", response.bucket);
        assertEquals("image.png", response.name);
        assertEquals(500000L, response.size);
        assertEquals("image/png", response.contentType);
        assertEquals("Profile picture", response.description);
        assertEquals("abc123def456", response.digest);
        assertEquals("SHA-256", response.digestAlgorithm);
        assertEquals(1, response.chunkCount);
        assertEquals(now, response.createdAt);
        assertEquals(now, response.updatedAt);
        assertEquals("value", response.headers.get("X-Custom-Header"));
    }

    @Test
    void testMetadataResponse_ChunkedFile() {
        ObjMetadataDto.Response response = new ObjMetadataDto.Response();

        response.name = "large-video.mp4";
        response.size = 100 * 1024 * 1024L; // 100MB
        response.contentType = "video/mp4";
        response.chunkCount = 100; // 100 chunks of 1MB each

        assertEquals(100 * 1024 * 1024L, response.size);
        assertEquals(100, response.chunkCount);
    }

    @Test
    void testWatchEvent_PutOperation() {
        ObjMetadataDto.WatchEvent event = new ObjMetadataDto.WatchEvent();

        event.type = "PUT";
        event.bucket = "uploads";
        event.name = "file.txt";
        event.size = 1000L;
        event.digest = "sha256hash";
        event.timestamp = OffsetDateTime.now();

        assertEquals("PUT", event.type);
        assertEquals("uploads", event.bucket);
        assertEquals("file.txt", event.name);
        assertEquals(1000L, event.size);
        assertEquals("sha256hash", event.digest);
        assertNotNull(event.timestamp);
    }

    @Test
    void testWatchEvent_DeleteOperation() {
        ObjMetadataDto.WatchEvent event = new ObjMetadataDto.WatchEvent();

        event.type = "DELETE";
        event.bucket = "uploads";
        event.name = "deleted-file.txt";
        event.size = null; // Size not relevant for delete
        event.digest = null;
        event.timestamp = OffsetDateTime.now();

        assertEquals("DELETE", event.type);
        assertNull(event.size);
        assertNull(event.digest);
    }

    @Test
    void testMetadataResponse_NoHeaders() {
        ObjMetadataDto.Response response = new ObjMetadataDto.Response();

        response.name = "simple.txt";
        response.size = 100L;
        response.headers = null;

        assertNull(response.headers);
    }

    @Test
    void testMetadataResponse_EmptyHeaders() {
        ObjMetadataDto.Response response = new ObjMetadataDto.Response();

        response.name = "simple.txt";
        response.size = 100L;
        response.headers = new HashMap<>();

        assertTrue(response.headers.isEmpty());
    }

    @Test
    void testObjectInfo_VeryLargeFile() {
        ObjMetadataDto.ObjectInfo info = new ObjMetadataDto.ObjectInfo();

        info.name = "extremely-large-file.bin";
        info.size = 500L * 1024L * 1024L * 1024L; // 500GB
        info.contentType = "application/octet-stream";

        assertEquals(500L * 1024L * 1024L * 1024L, info.size);
    }
}
