package space.maatini.kvstore;

import space.maatini.kvstore.dto.KvBucketDto;
import space.maatini.kvstore.dto.KvEntryDto;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for KV Store REST API.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KvStoreResourceTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String TEST_KEY = "test-key";
    private static final String TEST_VALUE = "Hello, World!";

    @Test
    @Order(1)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testCreateBucket() {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = BUCKET_NAME;
        request.description = "Test bucket";

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v1/kv/buckets")
                .then()
                .statusCode(201)
                .body("name", equalTo(BUCKET_NAME))
                .body("id", notNullValue());
    }

    @Test
    @Order(2)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testListBuckets() {
        given()
                .when()
                .get("/api/v1/kv/buckets")
                .then()
                .statusCode(200)
                .body("$", hasSize(greaterThanOrEqualTo(1)))
                .body("name", hasItem(BUCKET_NAME));
    }

    @Test
    @Order(3)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testGetBucket() {
        given()
                .when()
                .get("/api/v1/kv/buckets/{name}", BUCKET_NAME)
                .then()
                .statusCode(200)
                .body("name", equalTo(BUCKET_NAME));
    }

    @Test
    @Order(4)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testPutKey() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = TEST_VALUE;
        request.base64 = false;

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .put("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(200)
                .body("key", equalTo(TEST_KEY))
                .body("revision", equalTo(1))
                .body("operation", equalTo("PUT"));
    }

    @Test
    @Order(5)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testGetKey() {
        given()
                .when()
                .get("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(200)
                .body("key", equalTo(TEST_KEY))
                .body("value", notNullValue())
                .body("revision", equalTo(1));
    }

    @Test
    @Order(6)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testUpdateKey() {
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "Updated value";
        request.base64 = false;

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .put("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(200)
                .body("revision", equalTo(2));
    }

    @Test
    @Order(7)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testGetKeyHistory() {
        given()
                .queryParam("limit", 10)
                .when()
                .get("/api/v1/kv/buckets/{bucket}/keys/{key}/history", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(200)
                .body("$", hasSize(2))
                .body("revision", hasItems(2, 1));
    }

    @Test
    @Order(8)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testGetSpecificRevision() {
        given()
                .when()
                .get("/api/v1/kv/buckets/{bucket}/keys/{key}/revision/{revision}",
                        BUCKET_NAME, TEST_KEY, 1)
                .then()
                .statusCode(200)
                .body("revision", equalTo(1));
    }

    @Test
    @Order(9)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testDeleteKey() {
        given()
                .when()
                .delete("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(200)
                .body("operation", equalTo("DELETE"))
                .body("revision", equalTo(3));
    }

    @Test
    @Order(10)
    @TestSecurity(user = "user", roles = { "kv-read" })
    public void testGetDeletedKey() {
        given()
                .when()
                .get("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, TEST_KEY)
                .then()
                .statusCode(404);
    }

    @Test
    @Order(11)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testPurgeKey() {
        // First put a new value
        KvEntryDto.PutRequest request = new KvEntryDto.PutRequest();
        request.value = "New value";
        request.base64 = false;

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .put("/api/v1/kv/buckets/{bucket}/keys/{key}", BUCKET_NAME, "purge-key")
                .then()
                .statusCode(200);

        // Then purge it
        given()
                .when()
                .delete("/api/v1/kv/buckets/{bucket}/keys/{key}/purge", BUCKET_NAME, "purge-key")
                .then()
                .statusCode(200)
                .body("count", greaterThanOrEqualTo(1));
    }

    @Test
    @Order(100)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testDeleteBucket() {
        given()
                .when()
                .delete("/api/v1/kv/buckets/{name}", BUCKET_NAME)
                .then()
                .statusCode(204);
    }

    @Test
    @Order(101)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testBucketNotFound() {
        given()
                .when()
                .get("/api/v1/kv/buckets/{name}", "non-existent-bucket")
                .then()
                .statusCode(404)
                .body("error", equalTo("Not Found"));
    }

    @Test
    @Order(102)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void testDuplicateBucket() {
        KvBucketDto.CreateRequest request = new KvBucketDto.CreateRequest();
        request.name = "duplicate-test";

        // Create first
        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v1/kv/buckets")
                .then()
                .statusCode(201);

        // Try to create duplicate
        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v1/kv/buckets")
                .then()
                .statusCode(409)
                .body("error", equalTo("Conflict"));

        // Cleanup
        given()
                .when()
                .delete("/api/v1/kv/buckets/{name}", "duplicate-test")
                .then()
                .statusCode(204);
    }
}
