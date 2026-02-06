package space.maatini.objectstore.entity;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Object Store entities.
 */
class ObjEntityTest {

    // ==================== ObjBucket Tests ====================

    @Test
    void testObjBucket_Creation() {
        ObjBucket bucket = new ObjBucket();
        bucket.id = UUID.randomUUID();
        bucket.name = "files-bucket";
        bucket.description = "File storage bucket";
        bucket.chunkSize = 1024 * 1024; // 1MB
        bucket.maxObjectSize = 1024L * 1024L * 1024L; // 1GB
        bucket.createdAt = OffsetDateTime.now();

        assertNotNull(bucket.id);
        assertEquals("files-bucket", bucket.name);
        assertEquals("File storage bucket", bucket.description);
        assertEquals(1024 * 1024, bucket.chunkSize);
        assertEquals(1024L * 1024L * 1024L, bucket.maxObjectSize);
        assertNotNull(bucket.createdAt);
    }

    @Test
    void testObjBucket_DefaultValues() {
        ObjBucket bucket = new ObjBucket();

        // Fields without defaults
        assertNull(bucket.id);
        assertNull(bucket.name);
        assertNull(bucket.description);
        assertNull(bucket.createdAt);

        // Fields with defaults
        assertEquals(1048576, bucket.chunkSize); // 1MB default
        assertEquals(1073741824L, bucket.maxObjectSize); // 1GB default
    }

    // ==================== ObjMetadata Tests ====================

    @Test
    void testObjMetadata_Creation() {
        ObjMetadata metadata = new ObjMetadata();
        metadata.id = UUID.randomUUID();
        metadata.name = "document.pdf";
        metadata.size = 1024000L;
        metadata.contentType = "application/pdf";
        metadata.description = "Important document";
        metadata.digest = "sha256hashvalue";
        metadata.digestAlgorithm = "SHA-256";
        metadata.chunkCount = 1;
        metadata.createdAt = OffsetDateTime.now();
        metadata.updatedAt = OffsetDateTime.now();

        assertNotNull(metadata.id);
        assertEquals("document.pdf", metadata.name);
        assertEquals(1024000L, metadata.size);
        assertEquals("application/pdf", metadata.contentType);
        assertEquals("Important document", metadata.description);
        assertEquals("sha256hashvalue", metadata.digest);
        assertEquals("SHA-256", metadata.digestAlgorithm);
        assertEquals(1, metadata.chunkCount);
        assertNotNull(metadata.createdAt);
        assertNotNull(metadata.updatedAt);
    }

    @Test
    void testObjMetadata_WithBucketId() {
        UUID bucketId = UUID.randomUUID();

        ObjMetadata metadata = new ObjMetadata();
        metadata.bucketId = bucketId;
        metadata.name = "file.txt";

        assertEquals(bucketId, metadata.bucketId);
    }

    @Test
    void testObjMetadata_WithHeaders() {
        ObjMetadata metadata = new ObjMetadata();
        metadata.name = "file.txt";

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Custom-Header", "custom-value");
        headers.put("X-Upload-Id", "upload-123");
        metadata.headers = headers;

        assertNotNull(metadata.headers);
        assertEquals(2, metadata.headers.size());
        assertEquals("custom-value", metadata.headers.get("X-Custom-Header"));
        assertEquals("upload-123", metadata.headers.get("X-Upload-Id"));
    }

    @Test
    void testObjMetadata_ChunkedFile() {
        ObjMetadata metadata = new ObjMetadata();
        metadata.name = "large-video.mp4";
        metadata.size = 100L * 1024L * 1024L; // 100MB
        metadata.chunkCount = 100; // 100 x 1MB chunks

        assertEquals(100L * 1024L * 1024L, metadata.size);
        assertEquals(100, metadata.chunkCount);
    }

    @Test
    void testObjMetadata_DefaultDigestAlgorithm() {
        ObjMetadata metadata = new ObjMetadata();

        assertEquals("SHA-256", metadata.digestAlgorithm);
    }

    // ==================== ObjChunk Tests ====================

    @Test
    void testObjChunk_Creation() {
        ObjChunk chunk = new ObjChunk();
        chunk.id = UUID.randomUUID();
        chunk.chunkIndex = 0;
        chunk.data = "chunk data".getBytes();
        chunk.size = chunk.data.length;
        chunk.digest = "chunkdigest";

        assertNotNull(chunk.id);
        assertEquals(0, chunk.chunkIndex);
        assertNotNull(chunk.data);
        assertEquals(chunk.data.length, chunk.size);
        assertEquals("chunkdigest", chunk.digest);
    }

    @Test
    void testObjChunk_WithMetadataId() {
        UUID metadataId = UUID.randomUUID();

        ObjChunk chunk = new ObjChunk();
        chunk.metadataId = metadataId;
        chunk.chunkIndex = 5;
        chunk.data = new byte[1024];
        chunk.size = 1024;

        assertEquals(metadataId, chunk.metadataId);
        assertEquals(5, chunk.chunkIndex);
    }

    @Test
    void testObjChunk_Ordering() {
        ObjChunk chunk0 = new ObjChunk();
        chunk0.chunkIndex = 0;

        ObjChunk chunk1 = new ObjChunk();
        chunk1.chunkIndex = 1;

        ObjChunk chunk2 = new ObjChunk();
        chunk2.chunkIndex = 2;

        assertTrue(chunk0.chunkIndex < chunk1.chunkIndex);
        assertTrue(chunk1.chunkIndex < chunk2.chunkIndex);
    }

    @Test
    void testObjChunk_BinaryData() {
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }

        ObjChunk chunk = new ObjChunk();
        chunk.data = binaryData;
        chunk.size = binaryData.length;

        assertEquals(256, chunk.size);
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, chunk.data[i]);
        }
    }

    @Test
    void testObjChunk_FullSizeChunk() {
        int oneM = 1024 * 1024;
        byte[] data = new byte[oneM];

        ObjChunk chunk = new ObjChunk();
        chunk.data = data;
        chunk.size = oneM;

        assertEquals(oneM, chunk.size);
    }

    @Test
    void testObjChunk_PartialLastChunk() {
        ObjChunk chunk = new ObjChunk();
        chunk.chunkIndex = 5;
        chunk.data = new byte[500]; // Partial chunk
        chunk.size = 500;

        assertEquals(500, chunk.size);
    }

    // ==================== JsonbMapConverter Tests ====================

    @Test
    void testJsonbMapConverter_ConvertToDatabaseColumn() {
        JsonbMapConverter converter = new JsonbMapConverter();

        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");

        String json = converter.convertToDatabaseColumn(map);

        assertNotNull(json);
        assertTrue(json.contains("key1"));
        assertTrue(json.contains("value1"));
        assertTrue(json.contains("key2"));
        assertTrue(json.contains("value2"));
    }

    @Test
    void testJsonbMapConverter_ConvertToEntityAttribute() {
        JsonbMapConverter converter = new JsonbMapConverter();

        String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";

        Map<String, String> map = converter.convertToEntityAttribute(json);

        assertNotNull(map);
        assertEquals(2, map.size());
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
    }

    @Test
    void testJsonbMapConverter_NullInput() {
        JsonbMapConverter converter = new JsonbMapConverter();

        // null input returns null when writing to DB
        assertNull(converter.convertToDatabaseColumn(null));
        // null input returns empty map when reading from DB
        Map<String, String> result = converter.convertToEntityAttribute(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testJsonbMapConverter_EmptyMap() {
        JsonbMapConverter converter = new JsonbMapConverter();

        Map<String, String> emptyMap = new HashMap<>();
        // Empty map returns null when writing to DB
        assertNull(converter.convertToDatabaseColumn(emptyMap));
    }

    @Test
    void testJsonbMapConverter_EmptyJson() {
        JsonbMapConverter converter = new JsonbMapConverter();

        // Empty string returns empty map
        Map<String, String> map = converter.convertToEntityAttribute("");

        assertNotNull(map);
        assertTrue(map.isEmpty());
    }
}
