package space.maatini.objectstore.resource;

import space.maatini.objectstore.dto.ObjBucketDto;
import space.maatini.objectstore.entity.ObjBucket;
import space.maatini.objectstore.service.ObjectStoreService;
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
 * REST resource for Object Store bucket operations.
 */
@Path("/api/v1/objects/buckets")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Object Buckets", description = "Object Store bucket management")
public class ObjBucketResource {

    @Inject
    ObjectStoreService objectStoreService;

    @POST
    @Operation(summary = "Create a new object bucket", description = "Creates a new object bucket with the specified configuration")
    @APIResponses({
            @APIResponse(responseCode = "201", description = "Bucket created successfully", content = @Content(schema = @Schema(implementation = ObjBucketDto.Response.class))),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "409", description = "Bucket already exists")
    })
    @RolesAllowed({ "admin", "objects-write" })
    public Response createBucket(@Valid ObjBucketDto.CreateRequest request) {
        ObjBucket bucket = objectStoreService.createBucket(request);
        return Response.status(Response.Status.CREATED)
                .entity(ObjBucketDto.Response.from(bucket))
                .build();
    }

    @GET
    @Operation(summary = "List all object buckets", description = "Returns a list of all object buckets")
    @APIResponse(responseCode = "200", description = "List of buckets")
    @PermitAll
    public List<ObjBucketDto.Response> listBuckets() {
        return objectStoreService.listBuckets().stream()
                .map(ObjBucketDto.Response::from)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/{name}")
    @Operation(summary = "Get bucket details", description = "Returns details of a specific object bucket")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bucket details", content = @Content(schema = @Schema(implementation = ObjBucketDto.Response.class))),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @PermitAll
    public ObjBucketDto.Response getBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name) {
        return ObjBucketDto.Response.from(objectStoreService.getBucket(name));
    }

    @PUT
    @Path("/{name}")
    @Operation(summary = "Update bucket", description = "Updates object bucket configuration")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bucket updated"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin", "objects-write" })
    public ObjBucketDto.Response updateBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name,
            @Valid ObjBucketDto.UpdateRequest request) {
        return ObjBucketDto.Response.from(objectStoreService.updateBucket(name, request));
    }

    @DELETE
    @Path("/{name}")
    @Operation(summary = "Delete bucket", description = "Deletes an object bucket and all its objects")
    @APIResponses({
            @APIResponse(responseCode = "204", description = "Bucket deleted"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin" })
    public Response deleteBucket(
            @Parameter(description = "Bucket name") @PathParam("name") String name) {
        objectStoreService.deleteBucket(name);
        return Response.noContent().build();
    }
}
