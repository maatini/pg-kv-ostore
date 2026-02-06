package space.maatini.objectstore.service;

import space.maatini.common.exception.ConflictException;
import space.maatini.common.exception.NotFoundException;
import space.maatini.common.exception.ValidationException;
import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.entity.ObjChunk;
import space.maatini.objectstore.entity.ObjMetadata;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ObjectStoreService.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ObjectStoreServiceTest {

    @Inject
    ObjectStoreService objectStoreService;

    @InjectMock
    ObjectWatchService watchService;

    private static final String TEST_BUCKET = "unit-test-objects";
    private static final String TEST_OBJECT = "test-file.txt";
    private static final byte[] TEST_DATA = "This is test content for the object store.".getBytes();

    @BeforeEach
    void setUp() {
        Mockito.reset(watchService);
    }

    // ==================== Bucket Tests ====================

    @Test
    @Order(1)
    void testCreateBucket_Success() {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = TEST_BUCKET;
        request.description = "Test bucket for unit tests";
        request.chunkSize = 1024;
        request.maxObjectSize = 1024L * 1024L;

        ObjBucket bucket = objectStoreService.createBucket(request);

        assertNotNull(bucket);
        assertNotNull(bucket.id);
        assertEquals(TEST_BUCKET, bucket.name);
        assertEquals("Test bucket for unit tests", bucket.description);
        assertEquals(1024, bucket.chunkSize);
        assertEquals(1024L * 1024L, bucket.maxObjectSize);
    }

    @Test
    @Order(2)
    void testCreateBucket_DuplicateName() {
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = TEST_BUCKET;

        assertThrows(ConflictException.class, () -> objectStoreService.createBucket(request));
    }

    @Test
    @Order(3)
    void testGetBucket_Success() {
        ObjBucket bucket = objectStoreService.getBucket(TEST_BUCKET);

        assertNotNull(bucket);
        assertEquals(TEST_BUCKET, bucket.name);
    }

    @Test
    @Order(4)
    void testGetBucket_NotFound() {
        assertThrows(NotFoundException.class, () -> objectStoreService.getBucket("non-existent"));
    }

    @Test
    @Order(5)
    void testListBuckets() {
        List<ObjBucket> buckets = objectStoreService.listBuckets();

        assertNotNull(buckets);
        assertTrue(buckets.stream().anyMatch(b -> b.name.equals(TEST_BUCKET)));
    }

    @Test
    @Order(6)
    void testUpdateBucket() {
        ObjBucketDto.UpdateRequest request = new ObjBucketDto.UpdateRequest();
        request.description = "Updated description";
        request.chunkSize = 2048;

        ObjBucket bucket = objectStoreService.updateBucket(TEST_BUCKET, request);

        assertNotNull(bucket);
        assertEquals("Updated description", bucket.description);
        assertEquals(2048, bucket.chunkSize);
    }

    // ==================== Object Upload Tests ====================

    @Test
    @Order(10)
    void testPutObject_Success() throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(TEST_DATA);

        ObjMetadata metadata = objectStoreService.putObject(
                TEST_BUCKET, TEST_OBJECT, input, "text/plain", "Test file", null);

        assertNotNull(metadata);
        assertEquals(TEST_OBJECT, metadata.name);
        assertEquals(TEST_DATA.length, metadata.size);
        assertNotNull(metadata.digest);
        assertEquals("SHA-256", metadata.digestAlgorithm);
        assertEquals("text/plain", metadata.contentType);
        assertEquals("Test file", metadata.description);
        assertTrue(metadata.chunkCount >= 1);

        // Verify watcher was notified
        verify(watchService).notifyChange(any(ObjMetadataDto.WatchEvent.class));
    }

    @Test
    @Order(11)
    void testPutObject_VerifyDigest() throws IOException, NoSuchAlgorithmException {
        ByteArrayInputStream input = new ByteArrayInputStream(TEST_DATA);

        ObjMetadata metadata = objectStoreService.putObject(
                TEST_BUCKET, "digest-test.txt", input, "text/plain", null, null);

        // Calculate expected digest
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String expectedDigest = HexFormat.of().formatHex(md.digest(TEST_DATA));

        assertEquals(expectedDigest, metadata.digest);
    }

    @Test
    @Order(12)
    void testPutObject_WithChunking() throws IOException {
        // Create data larger than chunk size (chunk size is 2048 from update test)
        byte[] largeData = new byte[5000];
        new Random().nextBytes(largeData);

        ByteArrayInputStream input = new ByteArrayInputStream(largeData);

        ObjMetadata metadata = objectStoreService.putObject(
                TEST_BUCKET, "chunked-file.bin", input, "application/octet-stream", null, null);

        assertNotNull(metadata);
        assertEquals(largeData.length, metadata.size);
        assertTrue(metadata.chunkCount > 1);
    }

    @Test
    @Order(13)
    void testPutObject_ReplaceExisting() throws IOException {
        // First upload
        byte[] data1 = "First version".getBytes();
        objectStoreService.putObject(
                TEST_BUCKET, "replace-test.txt",
                new ByteArrayInputStream(data1), "text/plain", null, null);

        // Replace with new data
        byte[] data2 = "Second version".getBytes();
        ObjMetadata metadata = objectStoreService.putObject(
                TEST_BUCKET, "replace-test.txt",
                new ByteArrayInputStream(data2), "text/plain", null, null);

        assertEquals(data2.length, metadata.size);

        // Verify content was replaced
        byte[] retrieved = objectStoreService.getObjectData(TEST_BUCKET, "replace-test.txt");
        assertArrayEquals(data2, retrieved);
    }

    @Test
    @Order(14)
    void testPutObject_TooLarge() {
        // Create data larger than maxObjectSize (1MB from bucket creation)
        byte[] tooLargeData = new byte[1024 * 1024 + 1];
        ByteArrayInputStream input = new ByteArrayInputStream(tooLargeData);

        assertThrows(ValidationException.class, () -> objectStoreService.putObject(
                TEST_BUCKET, "too-large.bin", input, "application/octet-stream", null, null));
    }

    @Test
    @Order(15)
    void testPutObject_BucketNotFound() {
        ByteArrayInputStream input = new ByteArrayInputStream(TEST_DATA);

        assertThrows(NotFoundException.class, () -> objectStoreService.putObject(
                "non-existent", "file.txt", input, "text/plain", null, null));
    }

    @Test
    @Order(16)
    void testPutObject_WithCustomHeaders() throws IOException {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Custom-Header", "custom-value");
        headers.put("X-Another-Header", "another-value");

        ByteArrayInputStream input = new ByteArrayInputStream(TEST_DATA);

        ObjMetadata metadata = objectStoreService.putObject(
                TEST_BUCKET, "headers-test.txt", input, "text/plain", null, headers);

        assertNotNull(metadata.headers);
        assertEquals("custom-value", metadata.headers.get("X-Custom-Header"));
    }

    // ==================== Object Retrieval Tests ====================

    @Test
    @Order(20)
    void testGetMetadata_Success() {
        ObjMetadata metadata = objectStoreService.getMetadata(TEST_BUCKET, TEST_OBJECT);

        assertNotNull(metadata);
        assertEquals(TEST_OBJECT, metadata.name);
        assertEquals(TEST_DATA.length, metadata.size);
    }

    @Test
    @Order(21)
    void testGetMetadata_NotFound() {
        assertThrows(NotFoundException.class, () -> objectStoreService.getMetadata(TEST_BUCKET, "non-existent.txt"));
    }

    @Test
    @Order(22)
    void testListObjects() {
        List<ObjMetadata> objects = objectStoreService.listObjects(TEST_BUCKET);

        assertNotNull(objects);
        assertTrue(objects.size() >= 1);
        assertTrue(objects.stream().anyMatch(o -> o.name.equals(TEST_OBJECT)));
    }

    @Test
    @Order(23)
    void testGetObjectData_Success() throws IOException {
        byte[] data = objectStoreService.getObjectData(TEST_BUCKET, TEST_OBJECT);

        assertNotNull(data);
        assertArrayEquals(TEST_DATA, data);
    }

    @Test
    @Order(24)
    void testGetObjectChunks() {
        ObjectStoreService.ChunkIterator iterator = objectStoreService.getObjectChunks(TEST_BUCKET, TEST_OBJECT);

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());

        int totalSize = 0;
        while (iterator.hasNext()) {
            ObjChunk chunk = iterator.next();
            assertNotNull(chunk);
            assertNotNull(chunk.data);
            totalSize += chunk.size;
        }

        assertEquals(TEST_DATA.length, totalSize);
    }

    @Test
    @Order(25)
    void testGetObjectChunks_MultipleChunks() throws IOException {
        // Use the chunked file created earlier
        ObjectStoreService.ChunkIterator iterator = objectStoreService.getObjectChunks(TEST_BUCKET, "chunked-file.bin");

        assertNotNull(iterator);
        assertTrue(iterator.getTotalChunks() > 1);

        int chunkCount = 0;
        while (iterator.hasNext()) {
            iterator.next();
            chunkCount++;
        }

        assertEquals(iterator.getTotalChunks(), chunkCount);
    }

    // ==================== Integrity Verification Tests ====================

    @Test
    @Order(30)
    void testVerifyIntegrity_Success() {
        boolean valid = objectStoreService.verifyIntegrity(TEST_BUCKET, TEST_OBJECT);
        assertTrue(valid);
    }

    @Test
    @Order(31)
    void testVerifyIntegrity_LargeFile() throws IOException {
        boolean valid = objectStoreService.verifyIntegrity(TEST_BUCKET, "chunked-file.bin");
        assertTrue(valid);
    }

    // ==================== Object Deletion Tests ====================

    @Test
    @Order(40)
    void testDeleteObject_Success() {
        // First create an object to delete
        try {
            objectStoreService.putObject(
                    TEST_BUCKET, "to-delete.txt",
                    new ByteArrayInputStream("delete me".getBytes()),
                    "text/plain", null, null);
        } catch (IOException e) {
            fail("Failed to create object");
        }

        objectStoreService.deleteObject(TEST_BUCKET, "to-delete.txt");

        // Verify it's deleted
        assertThrows(NotFoundException.class, () -> objectStoreService.getMetadata(TEST_BUCKET, "to-delete.txt"));

        // Verify watcher was notified
        verify(watchService, atLeastOnce()).notifyChange(any(ObjMetadataDto.WatchEvent.class));
    }

    @Test
    @Order(41)
    void testDeleteObject_NotFound() {
        assertThrows(NotFoundException.class, () -> objectStoreService.deleteObject(TEST_BUCKET, "non-existent.txt"));
    }

    // ==================== Bucket Deletion Tests ====================

    @Test
    @Order(100)
    void testDeleteBucket() {
        objectStoreService.deleteBucket(TEST_BUCKET);

        assertThrows(NotFoundException.class, () -> objectStoreService.getBucket(TEST_BUCKET));
    }

    // ==================== Edge Cases ====================

    @Test
    @Order(110)
    void testPutObject_EmptyFile() throws IOException {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "empty-file-bucket";
        objectStoreService.createBucket(bucketRequest);

        ByteArrayInputStream input = new ByteArrayInputStream(new byte[0]);

        ObjMetadata metadata = objectStoreService.putObject(
                "empty-file-bucket", "empty.txt", input, "text/plain", null, null);

        assertEquals(0L, metadata.size);
        assertEquals(0, metadata.chunkCount);

        objectStoreService.deleteBucket("empty-file-bucket");
    }

    @Test
    @Order(111)
    void testPutObject_SpecialCharactersInName() throws IOException {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "special-names-bucket";
        objectStoreService.createBucket(bucketRequest);

        String specialName = "path/to/file with spaces (1).txt";
        ByteArrayInputStream input = new ByteArrayInputStream(TEST_DATA);

        ObjMetadata metadata = objectStoreService.putObject(
                "special-names-bucket", specialName, input, "text/plain", null, null);

        assertEquals(specialName, metadata.name);

        // Verify retrieval
        byte[] data = objectStoreService.getObjectData("special-names-bucket", specialName);
        assertArrayEquals(TEST_DATA, data);

        objectStoreService.deleteBucket("special-names-bucket");
    }

    @Test
    @Order(112)
    void testPutObject_BinaryData() throws IOException {
        ObjBucketDto.CreateRequest bucketRequest = new ObjBucketDto.CreateRequest();
        bucketRequest.name = "binary-bucket";
        objectStoreService.createBucket(bucketRequest);

        // Create binary data with all possible byte values
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }

        ByteArrayInputStream input = new ByteArrayInputStream(binaryData);

        ObjMetadata metadata = objectStoreService.putObject(
                "binary-bucket", "binary.bin", input, "application/octet-stream", null, null);

        assertEquals(256L, metadata.size);

        // Verify data integrity
        byte[] retrieved = objectStoreService.getObjectData("binary-bucket", "binary.bin");
        assertArrayEquals(binaryData, retrieved);

        objectStoreService.deleteBucket("binary-bucket");
    }
}
