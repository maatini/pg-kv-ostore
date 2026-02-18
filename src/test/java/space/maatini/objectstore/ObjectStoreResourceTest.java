package space.maatini.objectstore;

import space.maatini.objectstore.dto.ObjBucketDto;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for Object Store REST API.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ObjectStoreResourceTest {

        private static final String BUCKET_NAME = "test-objects";
        private static final String OBJECT_NAME = "test-file.txt";
        private static final String OBJECT_CONTENT = "This is the content of the test file.";

        @Test
        @Order(1)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testCreateBucket() {
                ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
                request.name = BUCKET_NAME;
                request.description = "Test object bucket";

                given()
                                .contentType(ContentType.JSON)
                                .body(request)
                                .when()
                                .post("/api/v1/objects/buckets")
                                .then()
                                .statusCode(201)
                                .body("name", equalTo(BUCKET_NAME))
                                .body("id", notNullValue());
        }

        @Test
        @Order(2)
        @TestSecurity(user = "user", roles = { "objects-read" })
        public void testListBuckets() {
                given()
                                .when()
                                .get("/api/v1/objects/buckets")
                                .then()
                                .statusCode(200)
                                .body("$", hasSize(greaterThanOrEqualTo(1)))
                                .body("name", hasItem(BUCKET_NAME));
        }

        @Test
        @Order(3)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testUploadObject() {
                given()
                                .contentType("text/plain")
                                .header("X-Object-Description", "Test file")
                                .body(OBJECT_CONTENT.getBytes())
                                .when()
                                .put("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .body("name", equalTo(OBJECT_NAME))
                                .body("size", equalTo(OBJECT_CONTENT.length()))
                                .body("digest", notNullValue())
                                .body("chunkCount", greaterThanOrEqualTo(1));
        }

        @Test
        @Order(4)
        @TestSecurity(user = "user", roles = { "objects-read" })
        public void testListObjects() {
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects", BUCKET_NAME)
                                .then()
                                .statusCode(200)
                                .body("$", hasSize(greaterThanOrEqualTo(1)))
                                .body("name", hasItem(OBJECT_NAME));
        }

        @Test
        @Order(5)
        @TestSecurity(user = "user", roles = { "objects-read" })
        public void testGetMetadata() {
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}/metadata",
                                                BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .body("name", equalTo(OBJECT_NAME))
                                .body("size", equalTo(OBJECT_CONTENT.length()))
                                .body("contentType", containsString("text/plain"))
                                .body("description", equalTo("Test file"));
        }

        @Test
        @Order(6)
        @TestSecurity(user = "user", roles = { "objects-read" })
        public void testDownloadObject() {
                String content = given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .header("X-Object-Digest", notNullValue())
                                .header("X-Object-Digest-Algorithm", equalTo("SHA-256"))
                                .extract()
                                .body()
                                .asString();

                Assertions.assertEquals(OBJECT_CONTENT, content);
        }

        @Test
        @Order(7)
        @TestSecurity(user = "user", roles = { "objects-read" })
        public void testVerifyIntegrity() {
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}/verify",
                                                BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .body("valid", equalTo(true))
                                .body("message", containsString("verified"));
        }

        @Test
        @Order(8)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testReplaceObject() {
                String newContent = "This is the updated content.";

                given()
                                .contentType("text/plain")
                                .body(newContent.getBytes())
                                .when()
                                .put("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .body("size", equalTo(newContent.length()));

                // Verify content was replaced
                String content = given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(200)
                                .extract()
                                .body()
                                .asString();

                Assertions.assertEquals(newContent, content);
        }

        @Test
        @Order(9)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testDeleteObject() {
                given()
                                .when()
                                .delete("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(204);

                // Verify deleted
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}/metadata",
                                                BUCKET_NAME, OBJECT_NAME)
                                .then()
                                .statusCode(404);
        }

        @Test
        @Order(10)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testUploadLargeObject() {
                // Create a larger object that will be chunked
                byte[] largeContent = new byte[1024 * 100]; // 100KB
                for (int i = 0; i < largeContent.length; i++) {
                        largeContent[i] = (byte) (i % 256);
                }

                given()
                                .contentType("application/octet-stream")
                                .body(largeContent)
                                .when()
                                .put("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, "large-file.bin")
                                .then()
                                .statusCode(200)
                                .body("size", equalTo(largeContent.length))
                                .body("chunkCount", greaterThanOrEqualTo(1));

                // Verify integrity
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{bucket}/objects/{name}/verify",
                                                BUCKET_NAME, "large-file.bin")
                                .then()
                                .statusCode(200)
                                .body("valid", equalTo(true));

                // Cleanup
                given()
                                .when()
                                .delete("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME,
                                                "large-file.bin")
                                .then()
                                .statusCode(204);
        }

        @Test
        @Order(100)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testDeleteBucket() {
                given()
                                .when()
                                .delete("/api/v1/objects/buckets/{name}", BUCKET_NAME)
                                .then()
                                .statusCode(204);
        }

        @Test
        @Order(101)
        @TestSecurity(user = "admin", roles = { "admin" })
        public void testBucketNotFound() {
                given()
                                .when()
                                .get("/api/v1/objects/buckets/{name}", "non-existent-bucket")
                                .then()
                                .statusCode(404);
        }
}
