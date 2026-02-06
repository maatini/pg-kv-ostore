package space.maatini.kvstore.resource;

import space.maatini.common.dto.ApiResponse;
import space.maatini.kvstore.dto.KvBucketDto;
import space.maatini.kvstore.entity.KvBucket;
import space.maatini.kvstore.service.KvService;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
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
 * REST resource for KV bucket operations.
 */
@Path("/api/v1/kv/buckets")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "KV Buckets", description = "Key-Value Store bucket management")
public class KvBucketResource {

    @Inject
    KvService kvService;

    @POST
    @Operation(summary = "Create a new bucket", description = "Creates a new KV bucket with the specified configuration")
    @APIResponses({
            @APIResponse(responseCode = "201", description = "Bucket created successfully", content = @Content(schema = @Schema(implementation = KvBucketDto.Response.class))),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "409", description = "Bucket already exists")
    })
    @RolesAllowed({ "admin", "kv-write" })
    public Response createBucket(@Valid KvBucketDto.CreateRequest request) {
        KvBucket bucket = kvService.createBucket(request);
        return Response.status(Response.Status.CREATED)
                .entity(KvBucketDto.Response.from(bucket))
                .build();
    }

    @GET
    @Operation(summary = "List all buckets", description = "Returns a list of all KV buckets")
    @APIResponse(responseCode = "200", description = "List of buckets")
    @PermitAll
    public List<KvBucketDto.Response> listBuckets() {
        return kvService.listBuckets().stream()
                .map(KvBucketDto.Response::from)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/{name}")
    @Operation(summary = "Get bucket details", description = "Returns details of a specific bucket")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bucket details", content = @Content(schema = @Schema(implementation = KvBucketDto.Response.class))),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @PermitAll
    public KvBucketDto.Response getBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name) {
        return KvBucketDto.Response.from(kvService.getBucket(name));
    }

    @PUT
    @Path("/{name}")
    @Operation(summary = "Update bucket", description = "Updates bucket configuration")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bucket updated"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin", "kv-write" })
    public KvBucketDto.Response updateBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name,
            @Valid KvBucketDto.UpdateRequest request) {
        return KvBucketDto.Response.from(kvService.updateBucket(name, request));
    }

    @DELETE
    @Path("/{name}")
    @Operation(summary = "Delete bucket", description = "Deletes a bucket and all its entries")
    @APIResponses({
            @APIResponse(responseCode = "204", description = "Bucket deleted"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin" })
    public Response deleteBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name) {
        kvService.deleteBucket(name);
        return Response.noContent().build();
    }

    @DELETE
    @Path("/{name}/purge")
    @Operation(summary = "Purge bucket", description = "Deletes all entries in a bucket but keeps the bucket")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bucket purged"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin", "kv-write" })
    public ApiResponse.Count purgeBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name) {
        long deleted = kvService.purgeBucket(name);
        return new ApiResponse.Count(deleted);
    }
}
