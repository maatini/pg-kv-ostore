package space.maatini.kvstore.resource;

import space.maatini.common.dto.ApiResponse;
import space.maatini.kvstore.dto.KvEntryDto;
import space.maatini.kvstore.entity.KvBucket;
import space.maatini.kvstore.entity.KvEntry;
import space.maatini.kvstore.service.KvService;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.stream.Collectors;

/**
 * REST resource for KV entry operations.
 */
@Path("/api/v1/kv/buckets/{bucket}/keys")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "KV Entries", description = "Key-Value Store entry operations")
public class KvEntryResource {

    @Inject
    KvService kvService;

    @GET
    @Operation(summary = "List all keys", description = "Returns a list of all keys in the bucket")
    @APIResponse(responseCode = "200", description = "List of keys")
    @PermitAll
    public List<String> listKeys(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket) {
        return kvService.listKeys(bucket);
    }

    @GET
    @Path("/{key}")
    @Operation(summary = "Get value", description = "Returns the latest value for a key")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Key value", content = @Content(schema = @Schema(implementation = KvEntryDto.Response.class))),
            @APIResponse(responseCode = "404", description = "Key not found")
    })
    @PermitAll
    public KvEntryDto.Response get(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key) {
        KvEntry entry = kvService.get(bucket, key);
        return KvEntryDto.Response.from(entry, bucket);
    }

    @GET
    @Path("/{key}/revision/{revision}")
    @Operation(summary = "Get specific revision", description = "Returns a specific revision of a key's value")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Key value at revision"),
            @APIResponse(responseCode = "404", description = "Revision not found")
    })
    @PermitAll
    public KvEntryDto.Response getRevision(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key,
            @Parameter(description = "Revision number") @PathParam("revision") Long revision) {
        KvEntry entry = kvService.getRevision(bucket, key, revision);
        return KvEntryDto.Response.from(entry, bucket);
    }

    @GET
    @Path("/{key}/history")
    @Operation(summary = "Get key history", description = "Returns the revision history for a key")
    @APIResponse(responseCode = "200", description = "Key history")
    @PermitAll
    public List<KvEntryDto.Response> getHistory(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key,
            @Parameter(description = "Maximum number of revisions") @QueryParam("limit") @DefaultValue("10") int limit) {
        return kvService.getHistory(bucket, key, limit).stream()
                .map(e -> KvEntryDto.Response.from(e, bucket))
                .collect(Collectors.toList());
    }

    @PUT
    @Path("/{key}")
    @Operation(summary = "Put value", description = "Sets a value for a key (creates new revision)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Value stored", content = @Content(schema = @Schema(implementation = KvEntryDto.Response.class))),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin", "kv-write" })
    public KvEntryDto.Response put(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key,
            KvEntryDto.PutRequest request) {
        KvEntry entry = kvService.put(bucket, key, request);
        return KvEntryDto.Response.from(entry, bucket);
    }

    @DELETE
    @Path("/{key}")
    @Operation(summary = "Delete key", description = "Marks a key as deleted (creates delete marker)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Key deleted"),
            @APIResponse(responseCode = "404", description = "Key not found")
    })
    @RolesAllowed({ "admin", "kv-write" })
    public KvEntryDto.Response delete(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key) {
        KvEntry entry = kvService.delete(bucket, key);
        return KvEntryDto.Response.from(entry, bucket);
    }

    @DELETE
    @Path("/{key}/purge")
    @Operation(summary = "Purge key", description = "Permanently deletes all revisions of a key")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Key purged"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin" })
    public ApiResponse.Count purge(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Key name") @PathParam("key") String key) {
        long deleted = kvService.purge(bucket, key);
        return new ApiResponse.Count(deleted);
    }
}
