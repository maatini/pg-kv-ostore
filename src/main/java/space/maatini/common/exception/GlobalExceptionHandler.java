package space.maatini.common.exception;

import space.maatini.common.dto.ApiResponse;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Global exception handler that maps exceptions to proper HTTP responses.
 */
@Provider
public class GlobalExceptionHandler {

    private static final Logger LOG = Logger.getLogger(GlobalExceptionHandler.class);

    @Context
    UriInfo uriInfo;

    /**
     * Handler for NotFoundException.
     */
    @Provider
    public static class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(NotFoundException e) {
            LOG.debugf("Not found: %s", e.getMessage());
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ApiResponse.Error(
                            404,
                            "Not Found",
                            e.getMessage(),
                            uriInfo != null ? uriInfo.getPath() : null))
                    .build();
        }
    }

    /**
     * Handler for ConflictException.
     */
    @Provider
    public static class ConflictExceptionMapper implements ExceptionMapper<ConflictException> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(ConflictException e) {
            LOG.debugf("Conflict: %s", e.getMessage());
            return Response.status(Response.Status.CONFLICT)
                    .entity(new ApiResponse.Error(
                            409,
                            "Conflict",
                            e.getMessage(),
                            uriInfo != null ? uriInfo.getPath() : null))
                    .build();
        }
    }

    /**
     * Handler for ValidationException.
     */
    @Provider
    public static class ValidationExceptionMapper implements ExceptionMapper<ValidationException> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(ValidationException e) {
            LOG.debugf("Validation error: %s", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ApiResponse.Error(
                            400,
                            "Bad Request",
                            e.getMessage(),
                            uriInfo != null ? uriInfo.getPath() : null))
                    .build();
        }
    }

    /**
     * Handler for ConstraintViolationException (Bean Validation).
     */
    @Provider
    public static class ConstraintViolationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(ConstraintViolationException e) {
            List<ApiResponse.FieldError> fieldErrors = e.getConstraintViolations().stream()
                    .map(v -> new ApiResponse.FieldError(
                            extractFieldName(v),
                            v.getMessage(),
                            v.getInvalidValue()))
                    .collect(Collectors.toList());

            ApiResponse.Error error = new ApiResponse.Error(
                    400,
                    "Validation Error",
                    "Request validation failed",
                    uriInfo != null ? uriInfo.getPath() : null);
            error.fieldErrors = fieldErrors;

            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(error)
                    .build();
        }

        private String extractFieldName(ConstraintViolation<?> violation) {
            String path = violation.getPropertyPath().toString();
            int lastDot = path.lastIndexOf('.');
            return lastDot >= 0 ? path.substring(lastDot + 1) : path;
        }
    }

    /**
     * Handler for IllegalArgumentException.
     */
    @Provider
    public static class IllegalArgumentExceptionMapper implements ExceptionMapper<IllegalArgumentException> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(IllegalArgumentException e) {
            LOG.debugf("Illegal argument: %s", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ApiResponse.Error(
                            400,
                            "Bad Request",
                            e.getMessage(),
                            uriInfo != null ? uriInfo.getPath() : null))
                    .build();
        }
    }

    /**
     * Generic handler for uncaught exceptions.
     */
    @Provider
    public static class GenericExceptionMapper implements ExceptionMapper<Exception> {
        @Context
        UriInfo uriInfo;

        @Override
        public Response toResponse(Exception e) {
            LOG.error("Unhandled exception", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ApiResponse.Error(
                            500,
                            "Internal Server Error",
                            "An unexpected error occurred",
                            uriInfo != null ? uriInfo.getPath() : null))
                    .build();
        }
    }
}
