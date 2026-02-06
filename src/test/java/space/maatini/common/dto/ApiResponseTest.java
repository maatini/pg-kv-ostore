package space.maatini.common.dto;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for common API response DTOs.
 */
class ApiResponseTest {

    @Test
    void testError_DefaultConstructor() {
        ApiResponse.Error error = new ApiResponse.Error();

        assertNotNull(error.timestamp);
    }

    @Test
    void testError_ThreeArgConstructor() {
        ApiResponse.Error error = new ApiResponse.Error(404, "Not Found", "Resource not found");

        assertEquals(404, error.status);
        assertEquals("Not Found", error.error);
        assertEquals("Resource not found", error.message);
        assertNotNull(error.timestamp);
    }

    @Test
    void testError_FourArgConstructor() {
        ApiResponse.Error error = new ApiResponse.Error(404, "Not Found", "Resource not found", "/api/v1/test");

        assertEquals(404, error.status);
        assertEquals("Not Found", error.error);
        assertEquals("Resource not found", error.message);
        assertEquals("/api/v1/test", error.path);
        assertNotNull(error.timestamp);
    }

    @Test
    void testError_AllStatusCodes() {
        ApiResponse.Error badRequest = new ApiResponse.Error(400, "Bad Request", "Invalid input");
        assertEquals(400, badRequest.status);

        ApiResponse.Error unauthorized = new ApiResponse.Error(401, "Unauthorized", "Auth required");
        assertEquals(401, unauthorized.status);

        ApiResponse.Error forbidden = new ApiResponse.Error(403, "Forbidden", "Access denied");
        assertEquals(403, forbidden.status);

        ApiResponse.Error notFound = new ApiResponse.Error(404, "Not Found", "Not found");
        assertEquals(404, notFound.status);

        ApiResponse.Error conflict = new ApiResponse.Error(409, "Conflict", "Already exists");
        assertEquals(409, conflict.status);

        ApiResponse.Error serverError = new ApiResponse.Error(500, "Internal Server Error", "Error");
        assertEquals(500, serverError.status);
    }

    @Test
    void testFieldError() {
        ApiResponse.FieldError fieldError = new ApiResponse.FieldError("name", "Name is required", null);

        assertEquals("name", fieldError.field);
        assertEquals("Name is required", fieldError.message);
        assertNull(fieldError.rejectedValue);
    }

    @Test
    void testFieldError_WithRejectedValue() {
        ApiResponse.FieldError fieldError = new ApiResponse.FieldError("age", "Must be positive", -5);

        assertEquals("age", fieldError.field);
        assertEquals("Must be positive", fieldError.message);
        assertEquals(-5, fieldError.rejectedValue);
    }

    @Test
    void testPagedList_Creation() {
        ApiResponse.PagedList<String> paged = new ApiResponse.PagedList<>(
                Arrays.asList("item1", "item2", "item3"),
                0, 10, 100L);

        assertEquals(3, paged.items.size());
        assertEquals(0, paged.page);
        assertEquals(10, paged.size);
        assertEquals(100L, paged.total);
        assertEquals(10, paged.totalPages);
        assertTrue(paged.hasMore);
    }

    @Test
    void testPagedList_SinglePage() {
        ApiResponse.PagedList<Integer> paged = new ApiResponse.PagedList<>(
                Arrays.asList(1, 2, 3, 4, 5),
                0, 10, 5L);

        assertEquals(5, paged.items.size());
        assertEquals(1, paged.totalPages);
        assertFalse(paged.hasMore);
    }

    @Test
    void testPagedList_LastPage() {
        ApiResponse.PagedList<String> paged = new ApiResponse.PagedList<>(
                Arrays.asList("item21", "item22"),
                2, 10, 22L // Third page, 2 items left
        );

        assertEquals(2, paged.page);
        assertEquals(3, paged.totalPages);
        assertFalse(paged.hasMore);
    }

    @Test
    void testPagedList_EmptyList() {
        ApiResponse.PagedList<String> paged = new ApiResponse.PagedList<>(
                Arrays.asList(),
                0, 10, 0L);

        assertTrue(paged.items.isEmpty());
        assertEquals(0L, paged.total);
        assertEquals(0, paged.totalPages);
        assertFalse(paged.hasMore);
    }

    @Test
    void testSuccess_DefaultConstructor() {
        ApiResponse.Success success = new ApiResponse.Success();

        assertTrue(success.success);
        assertNull(success.message);
    }

    @Test
    void testSuccess_WithMessage() {
        ApiResponse.Success success = new ApiResponse.Success("Operation completed");

        assertTrue(success.success);
        assertEquals("Operation completed", success.message);
    }

    @Test
    void testCount() {
        ApiResponse.Count count = new ApiResponse.Count(42);

        assertEquals(42, count.count);
    }

    @Test
    void testCount_Zero() {
        ApiResponse.Count count = new ApiResponse.Count(0);

        assertEquals(0, count.count);
    }

    @Test
    void testCount_LargeNumber() {
        ApiResponse.Count count = new ApiResponse.Count(1_000_000_000L);

        assertEquals(1_000_000_000L, count.count);
    }
}
