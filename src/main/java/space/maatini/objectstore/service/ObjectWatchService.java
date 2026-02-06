package space.maatini.objectstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import space.maatini.objectstore.dto.ObjMetadataDto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.Session;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Service for managing Object Store watch subscriptions and notifications.
 */
@ApplicationScoped
public class ObjectWatchService {

    private static final Logger LOG = Logger.getLogger(ObjectWatchService.class);

    @Inject
    ObjectMapper objectMapper;

    // Map of bucket -> Set of sessions watching that bucket
    private final Map<String, Set<Session>> bucketWatchers = new ConcurrentHashMap<>();

    // Map of bucket/objectName -> Set of sessions watching that specific object
    private final Map<String, Set<Session>> objectWatchers = new ConcurrentHashMap<>();

    // Map of session -> subscription info
    private final Map<Session, SubscriptionInfo> subscriptions = new ConcurrentHashMap<>();

    public static class SubscriptionInfo {
        public String bucket;
        public String objectName; // null for bucket-level watch

        public SubscriptionInfo(String bucket, String objectName) {
            this.bucket = bucket;
            this.objectName = objectName;
        }
    }

    /**
     * Subscribe a WebSocket session to watch a bucket.
     */
    public void subscribeBucket(Session session, String bucket) {
        LOG.debugf("Session %s subscribing to object bucket: %s", session.getId(), bucket);

        bucketWatchers.computeIfAbsent(bucket, k -> new CopyOnWriteArraySet<>()).add(session);
        subscriptions.put(session, new SubscriptionInfo(bucket, null));
    }

    /**
     * Subscribe a WebSocket session to watch a specific object.
     */
    public void subscribeObject(Session session, String bucket, String objectName) {
        String watchKey = bucket + "/" + objectName;
        LOG.debugf("Session %s subscribing to object: %s", session.getId(), watchKey);

        objectWatchers.computeIfAbsent(watchKey, k -> new CopyOnWriteArraySet<>()).add(session);
        subscriptions.put(session, new SubscriptionInfo(bucket, objectName));
    }

    /**
     * Unsubscribe a WebSocket session from all watches.
     */
    public void unsubscribe(Session session) {
        SubscriptionInfo info = subscriptions.remove(session);
        if (info != null) {
            if (info.objectName != null) {
                String watchKey = info.bucket + "/" + info.objectName;
                Set<Session> sessions = objectWatchers.get(watchKey);
                if (sessions != null) {
                    sessions.remove(session);
                    if (sessions.isEmpty()) {
                        objectWatchers.remove(watchKey);
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
            LOG.debugf("Session %s unsubscribed from objects", session.getId());
        }
    }

    /**
     * Notify watchers about a change event.
     */
    public void notifyChange(ObjMetadataDto.WatchEvent event) {
        LOG.debugf("Notifying object change: %s/%s (op=%s)", event.bucket, event.name, event.type);

        String jsonEvent;
        try {
            jsonEvent = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            LOG.error("Failed to serialize object watch event", e);
            return;
        }

        // Notify bucket watchers
        Set<Session> bucketSessions = bucketWatchers.get(event.bucket);
        if (bucketSessions != null) {
            for (Session session : bucketSessions) {
                sendToSession(session, jsonEvent);
            }
        }

        // Notify object watchers
        String watchKey = event.bucket + "/" + event.name;
        Set<Session> objectSessions = objectWatchers.get(watchKey);
        if (objectSessions != null) {
            for (Session session : objectSessions) {
                sendToSession(session, jsonEvent);
            }
        }
    }

    private void sendToSession(Session session, String jsonEvent) {
        if (!session.isOpen()) {
            unsubscribe(session);
            return;
        }

        try {
            session.getAsyncRemote().sendText(jsonEvent, result -> {
                if (!result.isOK()) {
                    LOG.warnf("Failed to send object event to session %s: %s",
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
}
