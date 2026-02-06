package space.maatini.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Common API response DTOs.
 */
public class ApiResponse {

    /**
     * Standard error response.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Error {
        public int status;
        public String error;
        public String message;
        public String path;
        public OffsetDateTime timestamp;
        public List<FieldError> fieldErrors;

        public Error() {
            this.timestamp = OffsetDateTime.now();
        }

        public Error(int status, String error, String message) {
            this();
            this.status = status;
            this.error = error;
            this.message = message;
        }

        public Error(int status, String error, String message, String path) {
            this(status, error, message);
            this.path = path;
        }
    }

    /**
     * Field validation error.
     */
    public static class FieldError {
        public String field;
        public String message;
        public Object rejectedValue;

        public FieldError(String field, String message, Object rejectedValue) {
            this.field = field;
            this.message = message;
            this.rejectedValue = rejectedValue;
        }
    }

    /**
     * Paginated list response.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PagedList<T> {
        public List<T> items;
        public int page;
        public int size;
        public long total;
        public int totalPages;
        public boolean hasMore;

        public PagedList(List<T> items, int page, int size, long total) {
            this.items = items;
            this.page = page;
            this.size = size;
            this.total = total;
            this.totalPages = (int) Math.ceil((double) total / size);
            this.hasMore = (page + 1) * size < total;
        }
    }

    /**
     * Simple success response.
     */
    public static class Success {
        public boolean success = true;
        public String message;

        public Success() {
        }

        public Success(String message) {
            this.message = message;
        }
    }

    /**
     * Count response.
     */
    public static class Count {
        public long count;

        public Count(long count) {
            this.count = count;
        }
    }
}
