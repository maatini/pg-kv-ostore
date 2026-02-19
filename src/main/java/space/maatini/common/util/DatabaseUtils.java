package space.maatini.common.util;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Utility for database operations, including RLS setup.
 */
@ApplicationScoped
public class DatabaseUtils {
    private static final Logger LOG = Logger.getLogger(DatabaseUtils.class);

    @Inject
    TenantContext tenantContext;

    /**
     * Sets the current tenant in the database session.
     * This must be called within a transaction.
     */
    public Uni<Void> setupTenant() {
        String tenantId = tenantContext.getTenantId();

        LOG.debugf("Setting app.current_tenant to %s (is_local=true)", tenantId);

        // Pass the tenantId directly (it can be null, which resets the setting for the
        // transaction)
        if (tenantId == null) {
            return Panache.getSession()
                    .flatMap(session -> session.createNativeQuery("SELECT set_config('app.current_tenant', NULL, true)")
                            .getSingleResult()
                            .onFailure().invoke(t -> LOG.errorf("Failed to clear tenant context: %s", t.getMessage()))
                            .replaceWithVoid());
        }

        return Panache.getSession()
                .flatMap(session -> session.createNativeQuery("SELECT set_config('app.current_tenant', :tenant, true)")
                        .setParameter("tenant", tenantId)
                        .getSingleResult()
                        .onFailure().invoke(t -> LOG.errorf("Failed to set tenant context: %s", t.getMessage()))
                        .replaceWithVoid());
    }
}
