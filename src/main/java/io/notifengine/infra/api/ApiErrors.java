package io.notifengine.infra.api;

import io.notifengine.common.ValidationError;
import io.notifengine.common.ValidationException;
import io.notifengine.core.CoreExceptions;
import io.notifengine.domain.notification.IllegalTransitionException;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

import java.util.List;

/** Maps application errors to RFC 7807-like JSON bodies with precise validation paths. */
public class ApiErrors {
    public record Problem(int status, String title, String detail, List<ValidationError> errors) {}

    @ServerExceptionMapper
    public Response validation(ValidationException e) {
        return Response.status(400).entity(new Problem(400, "validation failed", null, e.errors())).build();
    }

    @ServerExceptionMapper
    public Response notFound(CoreExceptions.NotFound e) { return Response.status(404).entity(new Problem(404, "not found", e.getMessage(), List.of())).build(); }

    @ServerExceptionMapper
    public Response conflict(CoreExceptions.Conflict e) { return Response.status(409).entity(new Problem(409, "conflict", e.getMessage(), List.of())).build(); }

    @ServerExceptionMapper
    public Response illegal(IllegalTransitionException e) {
        return Response.status(409).entity(new Problem(409, "illegal transition", e.getMessage(), List.of())).build();
    }

    @ServerExceptionMapper
    public Response badRequest(IllegalArgumentException e) {
        return Response.status(400).entity(new Problem(400, "bad request", e.getMessage(), List.of())).build();
    }
}
