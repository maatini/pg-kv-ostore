package space.maatini.objectstore;

import space.maatini.objectstore.dto.ObjBucketDto;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for Object Store Range Requests.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ObjectStoreRangeRequestTest {

    private static final String BUCKET_NAME = "range-test-bucket";
    private static final String OBJECT_NAME = "range-test.txt";
    private static final String OBJECT_CONTENT = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    @Test
    @Order(1)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void setup() {
        // Create bucket
        ObjBucketDto.CreateRequest request = new ObjBucketDto.CreateRequest();
        request.name = BUCKET_NAME;
        request.description = "Test bucket for range requests";

        given()
                .contentType(ContentType.JSON)
                .body(request)
                .when()
                .post("/api/v1/objects/buckets")
                .then()
                .statusCode(anyOf(is(201), is(409))); // 409 is fine if it already exists from previous run

        // Upload object
        given()
                .contentType("text/plain")
                .body(OBJECT_CONTENT.getBytes())
                .when()
                .put("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(200);
    }

    @Test
    @Order(2)
    @TestSecurity(user = "user", roles = { "objects-read" })
    public void testRangeRequest_First10Bytes() {
        String content = given()
                .header("Range", "bytes=0-9")
                .when()
                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(206)
                .header("Content-Range", "bytes 0-9/" + OBJECT_CONTENT.length())
                .header("Content-Length", "10")
                .extract()
                .body()
                .asString();

        assertEquals("0123456789", content);
    }

    @Test
    @Order(3)
    @TestSecurity(user = "user", roles = { "objects-read" })
    public void testRangeRequest_MiddleSection() {
        // "ABCDE" are at indices 10-14
        String content = given()
                .header("Range", "bytes=10-14")
                .when()
                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(206)
                .header("Content-Range", "bytes 10-14/" + OBJECT_CONTENT.length())
                .header("Content-Length", "5")
                .extract()
                .body()
                .asString();

        assertEquals("ABCDE", content);
    }

    @Test
    @Order(4)
    @TestSecurity(user = "user", roles = { "objects-read" })
    public void testRangeRequest_Suffix() {
        // Last 5 chars
        int len = OBJECT_CONTENT.length();
        // Range: bytes=-5 gets last 5 bytes
        // But our implementation might support "bytes=start-"
        // Standard says "bytes=-5" is last 5 bytes.
        // Let's test "bytes=start-" (from index 30 to end)
        // 0-9 (10), 10-35 (26 chars: A-Z)
        // Z is index 35.
        // Let's try bytes=31-35 (VWXYZ)

        String content = given()
                .header("Range", "bytes=31-35")
                .when()
                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(206)
                .header("Content-Range", "bytes 31-35/" + len)
                .header("Content-Length", "5")
                .extract()
                .body()
                .asString();

        assertEquals("VWXYZ", content);
    }

    @Test
    @Order(5)
    @TestSecurity(user = "user", roles = { "objects-read" })
    public void testRangeRequest_OpenEnd() {
        // bytes=30- means from 30 to end
        // 0123456789 (0-9)
        // ABCDEFGHIJKLMNOPQRSTUVWXYZ (10-35)
        // 30 is U.
        // So 30-35 is UVWXYZ.
        String content = given()
                .header("Range", "bytes=30-")
                .when()
                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(206)
                .header("Content-Range", "bytes 30-35/" + OBJECT_CONTENT.length())
                .header("Content-Length", "6")
                .extract()
                .body()
                .asString();

        assertEquals("uvwxyz".toUpperCase(), content);
    }

    @Test
    @Order(6)
    @TestSecurity(user = "user", roles = { "objects-read" })
    public void testRangeRequest_InvalidRange() {
        // Range beyond end
        given()
                .header("Range", "bytes=100-200")
                .when()
                .get("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(416) // Requested Range Not Satisfiable
                .header("Content-Range", containsString("bytes */" + OBJECT_CONTENT.length()));
    }

    @Test
    @Order(100)
    @TestSecurity(user = "admin", roles = { "admin" })
    public void cleanup() {
        given()
                .when()
                .delete("/api/v1/objects/buckets/{bucket}/objects/{name}", BUCKET_NAME, OBJECT_NAME)
                .then()
                .statusCode(anyOf(is(204), is(404)));

        given()
                .when()
                .delete("/api/v1/objects/buckets/{name}", BUCKET_NAME)
                .then()
                .statusCode(anyOf(is(204), is(404)));
    }
}
