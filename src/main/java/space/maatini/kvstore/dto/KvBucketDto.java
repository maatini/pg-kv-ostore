package space.maatini.kvstore.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * DTOs for Key-Value Store bucket operations.
 */
public class KvBucketDto {

    /**
     * Request to create a new bucket.
     */
    public static class CreateRequest {
        @NotBlank(message = "Bucket name is required")
        @Size(min = 1, max = 255, message = "Bucket name must be between 1 and 255 characters")
        public String name;

        public String description;
        public Integer maxValueSize;
        public Integer maxHistoryPerKey;
        public Long ttlSeconds;
    }

    /**
     * Request to update a bucket.
     */
    public static class UpdateRequest {
        public String description;
        public Integer maxValueSize;
        public Integer maxHistoryPerKey;
        public Long ttlSeconds;
    }

    /**
     * Bucket response DTO.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response {
        public UUID id;
        public String name;
        public String description;
        public Integer maxValueSize;
        public Integer maxHistoryPerKey;
        public Long ttlSeconds;
        public OffsetDateTime createdAt;
        public OffsetDateTime updatedAt;

        public static Response from(space.maatini.kvstore.entity.KvBucket bucket) {
            Response response = new Response();
            response.id = bucket.id;
            response.name = bucket.name;
            response.description = bucket.description;
            response.maxValueSize = bucket.maxValueSize;
            response.maxHistoryPerKey = bucket.maxHistoryPerKey;
            response.ttlSeconds = bucket.ttlSeconds;
            response.createdAt = bucket.createdAt;
            response.updatedAt = bucket.updatedAt;
            return response;
        }
    }
}
