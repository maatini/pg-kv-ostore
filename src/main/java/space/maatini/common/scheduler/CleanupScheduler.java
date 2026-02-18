package space.maatini.common.scheduler;

import io.smallrye.mutiny.Uni;
import space.maatini.kvstore.entity.KvEntry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Scheduled tasks for cleanup and maintenance.
 */
@ApplicationScoped
public class CleanupScheduler {

    private static final Logger LOG = Logger.getLogger(CleanupScheduler.class);

    /**
     * Clean up expired KV entries.
     * Runs every hour by default.
     */
    @Scheduled(every = "${kv.cleanup-interval:1h}")
    @io.quarkus.hibernate.reactive.panache.common.WithSession
    public Uni<Void> cleanupExpiredEntries() {
        LOG.debug("Running expired entries cleanup");

        OffsetDateTime now = OffsetDateTime.now();
        return KvEntry.find("expiresAt IS NOT NULL AND expiresAt < ?1", now)
                .list()
                .flatMap(expired -> {
                    if (expired.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    }

                    List<Uni<?>> deleteUnis = expired.stream()
                            .map(e -> ((KvEntry) e).delete())
                            .collect(Collectors.toList());

                    return Uni.combine().all().unis(deleteUnis)
                            .discardItems()
                            .invoke(() -> LOG.infof("Cleaned up %d expired entries", expired.size()));
                });
    }

    /**
     * Clean up stale watch subscriptions.
     * Runs every 5 minutes.
     */
    @Scheduled(every = "5m")
    public void cleanupStaleWatchers() {
        LOG.debug("Running stale watchers cleanup");
        // This is handled automatically by WebSocket session lifecycle
        // This method is a placeholder for any additional cleanup logic
    }
}
