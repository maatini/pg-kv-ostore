package space.maatini.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import space.maatini.kvstore.dto.KvEntryDto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.Session;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Service for managing KV watch subscriptions and notifications.
 */
@ApplicationScoped
public class KvWatchService {

    private static final Logger LOG = Logger.getLogger(KvWatchService.class);

    @Inject
    ObjectMapper objectMapper;

    // Map of bucket -> Set of sessions watching that bucket
    private final Map<String, Set<Session>> bucketWatchers = new ConcurrentHashMap<>();

    // Map of bucket/key -> Set of sessions watching that specific key
    private final Map<String, Set<Session>> keyWatchers = new ConcurrentHashMap<>();

    // Map of session -> subscription info
    private final Map<Session, SubscriptionInfo> subscriptions = new ConcurrentHashMap<>();

    public static class SubscriptionInfo {
        public String bucket;
        public String key; // null for bucket-level watch
        public long since; // revision to start from

        public SubscriptionInfo(String bucket, String key, long since) {
            this.bucket = bucket;
            this.key = key;
            this.since = since;
        }
    }

    /**
     * Subscribe a WebSocket session to watch a bucket.
     */
    public void subscribeBucket(Session session, String bucket, long since) {
        LOG.debugf("Session %s subscribing to bucket: %s (since=%d)",
                session.getId(), bucket, since);

        bucketWatchers.computeIfAbsent(bucket, k -> new CopyOnWriteArraySet<>()).add(session);
        subscriptions.put(session, new SubscriptionInfo(bucket, null, since));
    }

    /**
     * Subscribe a WebSocket session to watch a specific key.
     */
    public void subscribeKey(Session session, String bucket, String key, long since) {
        String watchKey = bucket + "/" + key;
        LOG.debugf("Session %s subscribing to key: %s (since=%d)",
                session.getId(), watchKey, since);

        keyWatchers.computeIfAbsent(watchKey, k -> new CopyOnWriteArraySet<>()).add(session);
        subscriptions.put(session, new SubscriptionInfo(bucket, key, since));
    }

    /**
     * Unsubscribe a WebSocket session from all watches.
     */
    public void unsubscribe(Session session) {
        SubscriptionInfo info = subscriptions.remove(session);
        if (info != null) {
            if (info.key != null) {
                String watchKey = info.bucket + "/" + info.key;
                Set<Session> sessions = keyWatchers.get(watchKey);
                if (sessions != null) {
                    sessions.remove(session);
                    if (sessions.isEmpty()) {
                        keyWatchers.remove(watchKey);
                    }
                }
            } else {
                Set<Session> sessions = bucketWatchers.get(info.bucket);
                if (sessions != null) {
                    sessions.remove(session);
                    if (sessions.isEmpty()) {
                        bucketWatchers.remove(info.bucket);
                    }
                }
            }
            LOG.debugf("Session %s unsubscribed", session.getId());
        }
    }

    /**
     * Notify watchers about a change event.
     */
    public void notifyChange(KvEntryDto.WatchEvent event) {
        LOG.debugf("Notifying change: %s/%s (op=%s, rev=%d)",
                event.bucket, event.key, event.type, event.revision);

        String jsonEvent;
        try {
            jsonEvent = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            LOG.error("Failed to serialize watch event", e);
            return;
        }

        // Notify bucket watchers
        Set<Session> bucketSessions = bucketWatchers.get(event.bucket);
        if (bucketSessions != null) {
            for (Session session : bucketSessions) {
                sendToSession(session, jsonEvent, event);
            }
        }

        // Notify key watchers
        String watchKey = event.bucket + "/" + event.key;
        Set<Session> keySessions = keyWatchers.get(watchKey);
        if (keySessions != null) {
            for (Session session : keySessions) {
                sendToSession(session, jsonEvent, event);
            }
        }
    }

    private void sendToSession(Session session, String jsonEvent, KvEntryDto.WatchEvent event) {
        if (!session.isOpen()) {
            unsubscribe(session);
            return;
        }

        SubscriptionInfo info = subscriptions.get(session);
        if (info != null && event.revision != null && event.revision <= info.since) {
            // Skip events before the "since" revision
            return;
        }

        try {
            session.getAsyncRemote().sendText(jsonEvent, result -> {
                if (!result.isOK()) {
                    LOG.warnf("Failed to send event to session %s: %s",
                            session.getId(), result.getException());
                }
            });
        } catch (Exception e) {
            LOG.warnf("Error sending to session %s: %s", session.getId(), e.getMessage());
            unsubscribe(session);
        }
    }

    /**
     * Get the number of active watchers.
     */
    public int getActiveWatcherCount() {
        return subscriptions.size();
    }

    /**
     * Get the number of watched buckets.
     */
    public int getWatchedBucketCount() {
        return bucketWatchers.size();
    }

    /**
     * Get the number of watched keys.
     */
    public int getWatchedKeyCount() {
        return keyWatchers.size();
    }
}
