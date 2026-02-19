package space.maatini.common.util;

import jakarta.enterprise.context.RequestScoped;

/**
 * Holder for the current tenant ID in the request scope.
 */
@RequestScoped
public class TenantContext {
    private String tenantId;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
