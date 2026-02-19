package space.maatini.common.filter;

import space.maatini.common.util.TenantContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Filter to extract tenant ID from request headers.
 */
@Provider
public class TenantFilter implements ContainerRequestFilter {

    @Inject
    TenantContext tenantContext;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String tenantId = requestContext.getHeaderString("X-Tenant-ID");
        if (tenantId != null) {
            tenantContext.setTenantId(tenantId);
        }
    }
}
