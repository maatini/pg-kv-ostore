package space.maatini.common.exception;

import space.maatini.common.dto.ApiResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for exception classes.
 */
class ExceptionHandlerTest {

    @Test
    void testNotFoundException() {
        NotFoundException exception = new NotFoundException("Resource not found");

        assertEquals("Resource not found", exception.getMessage());
    }

    @Test
    void testNotFoundException_WithCause() {
        Throwable cause = new RuntimeException("Underlying error");
        NotFoundException exception = new NotFoundException("Resource not found", cause);

        assertEquals("Resource not found", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testConflictException() {
        ConflictException exception = new ConflictException("Resource already exists");

        assertEquals("Resource already exists", exception.getMessage());
    }

    @Test
    void testConflictException_WithCause() {
        Throwable cause = new RuntimeException("Underlying error");
        ConflictException exception = new ConflictException("Resource already exists", cause);

        assertEquals("Resource already exists", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testValidationException() {
        ValidationException exception = new ValidationException("Invalid value");

        assertEquals("Invalid value", exception.getMessage());
    }

    @Test
    void testValidationException_WithCause() {
        Throwable cause = new RuntimeException("Underlying error");
        ValidationException exception = new ValidationException("Invalid value", cause);

        assertEquals("Invalid value", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testApiError_Creation() {
        ApiResponse.Error error = new ApiResponse.Error(404, "Not Found", "Resource not found");

        assertEquals(404, error.status);
        assertEquals("Not Found", error.error);
        assertEquals("Resource not found", error.message);
        assertNotNull(error.timestamp);
    }

    @Test
    void testApiError_WithPath() {
        ApiResponse.Error error = new ApiResponse.Error(404, "Not Found", "Resource not found", "/api/v1/test");

        assertEquals(404, error.status);
        assertEquals("/api/v1/test", error.path);
    }

    @Test
    void testApiError_DifferentStatusCodes() {
        // 400 Bad Request
        ApiResponse.Error badRequest = new ApiResponse.Error(400, "Bad Request", "Invalid input");
        assertEquals(400, badRequest.status);

        // 401 Unauthorized
        ApiResponse.Error unauthorized = new ApiResponse.Error(401, "Unauthorized", "Auth required");
        assertEquals(401, unauthorized.status);

        // 403 Forbidden
        ApiResponse.Error forbidden = new ApiResponse.Error(403, "Forbidden", "Access denied");
        assertEquals(403, forbidden.status);

        // 404 Not Found
        ApiResponse.Error notFound = new ApiResponse.Error(404, "Not Found", "Not found");
        assertEquals(404, notFound.status);

        // 409 Conflict
        ApiResponse.Error conflict = new ApiResponse.Error(409, "Conflict", "Already exists");
        assertEquals(409, conflict.status);

        // 500 Internal Server Error
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
}
