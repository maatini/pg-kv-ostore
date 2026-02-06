package space.maatini.objectstore.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * DTOs for Object Store bucket operations.
 */
public class ObjBucketDto {

    /**
     * Request to create a new object bucket.
     */
    public static class CreateRequest {
        @NotBlank(message = "Bucket name is required")
        @Size(min = 1, max = 255, message = "Bucket name must be between 1 and 255 characters")
        public String name;

        public String description;
        public Integer chunkSize;
        public Long maxObjectSize;
    }

    /**
     * Request to update a bucket.
     */
    public static class UpdateRequest {
        public String description;
        public Integer chunkSize;
        public Long maxObjectSize;
    }

    /**
     * Bucket response DTO.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response {
        public UUID id;
        public String name;
        public String description;
        public Integer chunkSize;
        public Long maxObjectSize;
        public OffsetDateTime createdAt;
        public OffsetDateTime updatedAt;

        public static Response from(space.maatini.objectstore.entity.ObjBucket bucket) {
            Response response = new Response();
            response.id = bucket.id;
            response.name = bucket.name;
            response.description = bucket.description;
            response.chunkSize = bucket.chunkSize;
            response.maxObjectSize = bucket.maxObjectSize;
            response.createdAt = bucket.createdAt;
            response.updatedAt = bucket.updatedAt;
            return response;
        }
    }
}
