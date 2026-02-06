package space.maatini.common.health;

import space.maatini.kvstore.entity.KvBucket;
import space.maatini.objectstore.entity.ObjBucket;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

/**
 * Health checks for the KV and Object Store service.
 */
@ApplicationScoped
public class StoreHealthCheck {

    /**
     * Liveness check - is the application alive?
     */
    @Liveness
    public HealthCheck liveness() {
        return () -> HealthCheckResponse.up("kv-ostore-alive");
    }

    /**
     * Readiness check - is the application ready to serve requests?
     */
    @Readiness
    public HealthCheck readiness() {
        return () -> {
            HealthCheckResponseBuilder builder = HealthCheckResponse.named("kv-ostore-ready");

            try {
                // Check database connectivity by counting buckets
                long kvBucketCount = KvBucket.count();
                long objBucketCount = ObjBucket.count();

                builder.up()
                        .withData("kv_buckets", kvBucketCount)
                        .withData("object_buckets", objBucketCount)
                        .withData("database", "connected");

            } catch (Exception e) {
                builder.down()
                        .withData("database", "disconnected")
                        .withData("error", e.getMessage());
            }

            return builder.build();
        };
    }
}
