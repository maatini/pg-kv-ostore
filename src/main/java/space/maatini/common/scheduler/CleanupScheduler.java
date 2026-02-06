package space.maatini.common.scheduler;

import space.maatini.kvstore.entity.KvEntry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.List;

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
    @Transactional
    public void cleanupExpiredEntries() {
        LOG.debug("Running expired entries cleanup");

        OffsetDateTime now = OffsetDateTime.now();
        List<KvEntry> expired = KvEntry.getEntityManager()
                .createNamedQuery("KvEntry.findExpired", KvEntry.class)
                .setParameter("now", now)
                .getResultList();

        if (!expired.isEmpty()) {
            for (KvEntry entry : expired) {
                entry.delete();
            }
            LOG.infof("Cleaned up %d expired entries", expired.size());
        }
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
