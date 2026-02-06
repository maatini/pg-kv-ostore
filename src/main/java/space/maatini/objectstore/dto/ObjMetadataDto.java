package space.maatini.objectstore.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import space.maatini.objectstore.entity.ObjMetadata;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTOs for Object Store metadata operations.
 */
public class ObjMetadataDto {

    /**
     * Object metadata response DTO.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response {
        public UUID id;
        public String bucket;
        public String name;
        public Long size;
        public Integer chunkCount;
        public String digest;
        public String digestAlgorithm;
        public String contentType;
        public String description;
        public Map<String, String> headers;
        public OffsetDateTime createdAt;
        public OffsetDateTime updatedAt;

        public static Response from(ObjMetadata metadata, String bucketName) {
            Response response = new Response();
            response.id = metadata.id;
            response.bucket = bucketName;
            response.name = metadata.name;
            response.size = metadata.size;
            response.chunkCount = metadata.chunkCount;
            response.digest = metadata.digest;
            response.digestAlgorithm = metadata.digestAlgorithm;
            response.contentType = metadata.contentType;
            response.description = metadata.description;
            response.headers = metadata.headers;
            response.createdAt = metadata.createdAt;
            response.updatedAt = metadata.updatedAt;
            return response;
        }
    }

    /**
     * Object info for listing (minimal metadata).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ObjectInfo {
        public String name;
        public Long size;
        public String digest;
        public String contentType;
        public OffsetDateTime createdAt;

        public static ObjectInfo from(ObjMetadata metadata) {
            ObjectInfo info = new ObjectInfo();
            info.name = metadata.name;
            info.size = metadata.size;
            info.digest = metadata.digest;
            info.contentType = metadata.contentType;
            info.createdAt = metadata.createdAt;
            return info;
        }
    }

    /**
     * Watch event for object changes.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WatchEvent {
        public String type; // PUT, DELETE
        public String bucket;
        public String name;
        public Long size;
        public String digest;
        public OffsetDateTime timestamp;

        public static WatchEvent fromPut(ObjMetadata metadata, String bucketName) {
            WatchEvent event = new WatchEvent();
            event.type = "PUT";
            event.bucket = bucketName;
            event.name = metadata.name;
            event.size = metadata.size;
            event.digest = metadata.digest;
            event.timestamp = metadata.createdAt;
            return event;
        }

        public static WatchEvent fromDelete(String bucketName, String objectName) {
            WatchEvent event = new WatchEvent();
            event.type = "DELETE";
            event.bucket = bucketName;
            event.name = objectName;
            event.timestamp = OffsetDateTime.now();
            return event;
        }
    }
}
