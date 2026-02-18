package space.maatini.kvstore.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import io.vertx.pgclient.PgConnectOptions;
import space.maatini.kvstore.dto.KvEntryDto;
import space.maatini.kvstore.entity.KvBucket;
import space.maatini.kvstore.entity.KvEntry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.websocket.Session;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Service for managing KV watch subscriptions and notifications via PostgreSQL
 * LISTEN/NOTIFY.
 */
@ApplicationScoped
public class KvWatchService {

    private static final Logger LOG = Logger.getLogger(KvWatchService.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    Vertx vertx;

    @ConfigProperty(name = "quarkus.datasource.reactive.url")
    String reactiveUrl;

    @ConfigProperty(name = "quarkus.datasource.username")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    String password;

    private PgSubscriber subscriber;

    // Cache bucket ID <-> Name mappings
    // We need ID -> Name to dispatch events (trigger sends ID, clients watch Name)
    private final Map<UUID, String> bucketIdToName = new ConcurrentHashMap<>();

    // Map of bucketName -> Set of sessions watching that bucket
    private final Map<String, Set<Session>> bucketWatchers = new ConcurrentHashMap<>();

    // Map of bucketName/key -> Set of sessions watching that specific key
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

    void onStart(@Observes StartupEvent ev) {
        // Dispatch to Vert.x context because Hibernate Reactive requires it
        vertx.getOrCreateContext().runOnContext(() -> {
            // 1. Load existing buckets to cache
            loadBuckets()
                    .subscribe().with(
                            v -> LOG.info("Loaded buckets for watch service"),
                            t -> LOG.error("Failed to load buckets", t));

            // 2. Start PostgreSQL Subscriber
            startSubscriber();
        });
    }

    private Uni<Void> loadBuckets() {
        return KvBucket.listAll()
                .onItem().transformToUni(list -> {
                    for (Object obj : list) {
                        KvBucket b = (KvBucket) obj;
                        bucketIdToName.put(b.id, b.name);
                    }
                    return Uni.createFrom().voidItem();
                });
    }

    private void startSubscriber() {
        // Strip 'vertx-reactive:' prefix if present to get standard Postgres URI
        String url = reactiveUrl.replace("vertx-reactive:", "");

        PgConnectOptions options = PgConnectOptions.fromUri(url)
                .setUser(username)
                .setPassword(password)
                .setReconnectAttempts(10)
                .setReconnectInterval(1000);

        subscriber = PgSubscriber.subscriber(vertx, options);

        subscriber.connect()
                .onItem().invoke(() -> {
                    LOG.info("Connected to PostgreSQL for LISTEN/NOTIFY");
                    subscriber.channel("store_changes")
                            .handler(payload -> handleNotification(payload));
                })
                .onFailure().invoke(t -> LOG.error("Failed to connect PgSubscriber", t))
                .subscribe().with(v -> {
                }, t -> {
                });
    }

    private void handleNotification(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            String table = root.get("table").asText();
            String action = root.get("action").asText();

            // Only care about INSERT/UPDATE/DELETE (mostly INSERT for PUT)
            // 'new' might be missing on DELETE (use 'old'?)
            JsonNode data = root.has("new") ? root.get("new") : root.get("old");

            if (data == null)
                return;

            // Handle KvEntry changes
            if (table.startsWith("kv_entries")) {
                UUID bucketId = UUID.fromString(data.get("bucket_id").asText());
                String bucketName = bucketIdToName.get(bucketId);

                if (bucketName == null) {
                    // Possible new bucket, ignore or fetch?
                    // For now, ignore as likely race condition or deleted bucket
                    return;
                }

                String key = data.get("key").asText();
                long revision = data.get("revision").asLong();
                String operation = data.get("operation").asText();

                // Construct WatchEvent
                KvEntryDto.WatchEvent event = new KvEntryDto.WatchEvent();
                event.bucket = bucketName;
                event.key = key;
                event.revision = revision;
                event.type = operation;

                notifyChange(event);
            }
            // Handle Object Metadata changes if needed (future)

        } catch (Exception e) {
            LOG.error("Failed to handle notification: " + payload, e);
        }
    }

    // Public for testing and internal dispatch
    public void notifyChange(KvEntryDto.WatchEvent event) {
        LOG.debugf("Broadcasting event: %s/%s (op=%s, rev=%d)",
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

    // ... Subscription methods remain mostly same, but we might want to ensure
    // bucket cache is updated on sub

    /**
     * Subscribe a WebSocket session to watch a bucket.
     */
    public void subscribeBucket(Session session, String bucket, long since) {
        // Ensure we load the bucket ID if not known?
        // Actually, if client subscribes to non-existent bucket, that's fine,
        // but if bucket is created later, we need to know its ID.
        // It's better to reload buckets periodically or use lazy loading.
        // For prototype, initial load + optimistic is fine.
        // Or if we miss it, we miss it (until restart).
        // Let's rely on initial load for now.

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

    // For testing/internal usage
    public void setBucketName(UUID id, String name) {
        bucketIdToName.put(id, name);
    }

    public void clear() {
        bucketWatchers.clear();
        keyWatchers.clear();
        subscriptions.clear();
        bucketIdToName.clear();
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
