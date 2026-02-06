package space.maatini.objectstore.resource;

import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.entity.ObjChunk;
import space.maatini.objectstore.entity.ObjMetadata;
import space.maatini.objectstore.service.ObjectStoreService;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST resource for Object Store object operations.
 */
@Path("/api/v1/objects/buckets/{bucket}/objects")
@Tag(name = "Objects", description = "Object Store object operations")
public class ObjectResource {

    @Inject
    ObjectStoreService objectStoreService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List all objects", description = "Returns a list of all objects in the bucket")
    @APIResponse(responseCode = "200", description = "List of objects")
    @PermitAll
    public List<ObjMetadataDto.ObjectInfo> listObjects(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket) {
        return objectStoreService.listObjects(bucket).stream()
                .map(ObjMetadataDto.ObjectInfo::from)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/{name}/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get object metadata", description = "Returns metadata for an object")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Object metadata", content = @Content(schema = @Schema(implementation = ObjMetadataDto.Response.class))),
            @APIResponse(responseCode = "404", description = "Object not found")
    })
    @PermitAll
    public ObjMetadataDto.Response getMetadata(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Object name") @PathParam("name") String name) {
        ObjMetadata metadata = objectStoreService.getMetadata(bucket, name);
        return ObjMetadataDto.Response.from(metadata, bucket);
    }

    @GET
    @Path("/{name}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Operation(summary = "Download object", description = "Downloads the object data as a stream")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Object data"),
            @APIResponse(responseCode = "404", description = "Object not found")
    })
    @PermitAll
    public Response downloadObject(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Object name") @PathParam("name") String name) {

        ObjMetadata metadata = objectStoreService.getMetadata(bucket, name);
        ObjectStoreService.ChunkIterator chunks = objectStoreService.getObjectChunks(bucket, name);

        // Stream the chunks
        StreamingOutput stream = output -> {
            while (chunks.hasNext()) {
                ObjChunk chunk = chunks.next();
                output.write(chunk.data);
            }
            output.flush();
        };

        Response.ResponseBuilder response = Response.ok(stream)
                .header("Content-Length", metadata.size)
                .header("Content-Disposition", "attachment; filename=\"" + name + "\"")
                .header("X-Object-Digest", metadata.digest)
                .header("X-Object-Digest-Algorithm", metadata.digestAlgorithm);

        if (metadata.contentType != null) {
            response.header("Content-Type", metadata.contentType);
        }

        return response.build();
    }

    @PUT
    @Path("/{name}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Upload object", description = "Uploads an object (replaces if exists)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Object uploaded", content = @Content(schema = @Schema(implementation = ObjMetadataDto.Response.class))),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "404", description = "Bucket not found")
    })
    @RolesAllowed({ "admin", "objects-write" })
    public ObjMetadataDto.Response uploadObject(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Object name") @PathParam("name") String name,
            @HeaderParam("Content-Type") String contentType,
            @HeaderParam("X-Object-Description") String description,
            InputStream data) throws Exception {

        // Collect custom headers
        Map<String, String> headers = new HashMap<>();
        // Could add more custom header handling here

        ObjMetadata metadata = objectStoreService.putObject(
                bucket, name, data, contentType, description, headers);

        return ObjMetadataDto.Response.from(metadata, bucket);
    }

    @DELETE
    @Path("/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Delete object", description = "Deletes an object and all its chunks")
    @APIResponses({
            @APIResponse(responseCode = "204", description = "Object deleted"),
            @APIResponse(responseCode = "404", description = "Object not found")
    })
    @RolesAllowed({ "admin", "objects-write" })
    public Response deleteObject(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Object name") @PathParam("name") String name) {
        objectStoreService.deleteObject(bucket, name);
        return Response.noContent().build();
    }

    @GET
    @Path("/{name}/verify")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Verify object integrity", description = "Verifies the object's data integrity using its digest")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Integrity check result"),
            @APIResponse(responseCode = "404", description = "Object not found")
    })
    @PermitAll
    public IntegrityResult verifyIntegrity(
            @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
            @Parameter(description = "Object name") @PathParam("name") String name) {
        boolean valid = objectStoreService.verifyIntegrity(bucket, name);
        return new IntegrityResult(valid);
    }

    public static class IntegrityResult {
        public boolean valid;
        public String message;

        public IntegrityResult(boolean valid) {
            this.valid = valid;
            this.message = valid ? "Object integrity verified" : "Object integrity check failed";
        }
    }
}
