package space.maatini.objectstore.resource;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import space.maatini.objectstore.dto.ObjMetadataDto;
import space.maatini.objectstore.service.ObjectStoreService;
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
        public Uni<List<ObjMetadataDto.ObjectInfo>> listObjects(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket) {
                return objectStoreService.listObjects(bucket)
                                .map(list -> list.stream()
                                                .map(ObjMetadataDto.ObjectInfo::from)
                                                .collect(Collectors.toList()));
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
        public Uni<ObjMetadataDto.Response> getMetadata(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
                        @Parameter(description = "Object name") @PathParam("name") String name) {
                return objectStoreService.getMetadata(bucket, name)
                                .map(metadata -> ObjMetadataDto.Response.from(metadata, bucket));
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
        public Uni<Response> downloadObject(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
                        @Parameter(description = "Object name") @PathParam("name") String name) {

                return objectStoreService.getMetadata(bucket, name)
                                .flatMap(metadata -> objectStoreService.getObjectData(bucket, name)
                                                .map(data -> {
                                                        Response.ResponseBuilder response = Response.ok(data)
                                                                        .header("Content-Length", metadata.size)
                                                                        .header("Content-Disposition",
                                                                                        "attachment; filename=\"" + name
                                                                                                        + "\"")
                                                                        .header("X-Object-Digest", metadata.digest)
                                                                        .header("X-Object-Digest-Algorithm",
                                                                                        metadata.digestAlgorithm);

                                                        if (metadata.contentType != null) {
                                                                response.header("Content-Type", metadata.contentType);
                                                        }

                                                        return response.build();
                                                }));
        }

        @PUT
        @Path("/{name}")
        @Consumes(MediaType.WILDCARD)
        @Produces(MediaType.APPLICATION_JSON)
        @Operation(summary = "Upload object", description = "Uploads an object (replaces if exists)")
        @APIResponses({
                        @APIResponse(responseCode = "200", description = "Object uploaded", content = @Content(schema = @Schema(implementation = ObjMetadataDto.Response.class))),
                        @APIResponse(responseCode = "400", description = "Invalid request"),
                        @APIResponse(responseCode = "404", description = "Bucket not found")
        })
        @RolesAllowed({ "admin", "objects-write" })
        public Uni<ObjMetadataDto.Response> uploadObject(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
                        @Parameter(description = "Object name") @PathParam("name") String name,
                        @HeaderParam("Content-Type") String contentType,
                        @HeaderParam("X-Object-Description") String description,
                        byte[] data) {

                // Collect custom headers
                Map<String, String> headers = new HashMap<>();
                if (description != null) {
                        headers.put("X-Object-Description", description);
                }
                // Could add more custom header handling here

                Multi<byte[]> dataStream = Multi.createFrom().item(data);
                return objectStoreService.putObject(bucket, name, dataStream, contentType, description, headers)
                                .map(metadata -> ObjMetadataDto.Response.from(metadata, bucket));
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
        public Uni<Response> deleteObject(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
                        @Parameter(description = "Object name") @PathParam("name") String name) {
                return objectStoreService.deleteObject(bucket, name).replaceWith(Response.noContent().build());
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
        public Uni<IntegrityResult> verifyIntegrity(
                        @Parameter(description = "Bucket name") @PathParam("bucket") String bucket,
                        @Parameter(description = "Object name") @PathParam("name") String name) {
                return objectStoreService.verifyIntegrity(bucket, name).map(IntegrityResult::new);
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
