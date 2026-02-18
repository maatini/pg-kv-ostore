package space.maatini.common.health;

import space.maatini.kvstore.entity.KvBucket;
import space.maatini.objectstore.entity.ObjBucket;
import jakarta.enterprise.context.ApplicationScoped;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.health.*;
import io.smallrye.health.api.AsyncHealthCheck;

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
    public AsyncHealthCheck readiness() {
        return () -> {
            HealthCheckResponseBuilder builder = HealthCheckResponse.named("kv-ostore-ready");

            return Uni.combine().all().unis(
                    KvBucket.count(),
                    ObjBucket.count()).asTuple()
                    .onItem().transform(counts -> builder.up()
                            .withData("kv_buckets", counts.getItem1())
                            .withData("object_buckets", counts.getItem2())
                            .withData("database", "connected")
                            .build())
                    .onFailure().recoverWithItem(e -> builder.down()
                            .withData("database", "disconnected")
                            .withData("error", e.getMessage())
                            .build());
        };
    }
}
