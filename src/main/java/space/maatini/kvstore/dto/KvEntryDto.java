package space.maatini.kvstore.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import space.maatini.kvstore.entity.KvEntry;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.UUID;

/**
 * DTOs for Key-Value Store entry operations.
 */
public class KvEntryDto {

    /**
     * Request to put a key-value pair.
     */
    public static class PutRequest {
        public String value; // Base64 encoded or plain string
        public boolean base64; // If true, value is base64 encoded
        public Long ttlSeconds; // Optional TTL for auto-expiration
    }

    /**
     * Entry response DTO.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response {
        public UUID id;
        public String bucket;
        public String key;
        public String value; // Base64 encoded
        public Long revision;
        public String operation;
        public OffsetDateTime createdAt;
        public OffsetDateTime expiresAt;

        public static Response from(KvEntry entry, String bucketName) {
            Response response = new Response();
            response.id = entry.id;
            response.bucket = bucketName;
            response.key = entry.key;
            if (entry.value != null) {
                response.value = Base64.getEncoder().encodeToString(entry.value);
            }
            response.revision = entry.revision;
            response.operation = entry.operation.name();
            response.createdAt = entry.createdAt;
            response.expiresAt = entry.expiresAt;
            return response;
        }
    }

    /**
     * Key entry info (without value, for listing).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class KeyInfo {
        public String key;
        public Long revision;
        public String operation;
        public OffsetDateTime createdAt;

        public static KeyInfo from(KvEntry entry) {
            KeyInfo info = new KeyInfo();
            info.key = entry.key;
            info.revision = entry.revision;
            info.operation = entry.operation.name();
            info.createdAt = entry.createdAt;
            return info;
        }
    }

    /**
     * Watch event DTO.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WatchEvent {
        public String type; // PUT, DELETE, PURGE
        public String bucket;
        public String key;
        public String value; // Base64 encoded (null for DELETE)
        public Long revision;
        public OffsetDateTime timestamp;

        public static WatchEvent from(KvEntry entry, String bucketName) {
            WatchEvent event = new WatchEvent();
            event.type = entry.operation.name();
            event.bucket = bucketName;
            event.key = entry.key;
            if (entry.value != null && entry.operation != KvEntry.Operation.DELETE) {
                event.value = Base64.getEncoder().encodeToString(entry.value);
            }
            event.revision = entry.revision;
            event.timestamp = entry.createdAt;
            return event;
        }
    }
}
