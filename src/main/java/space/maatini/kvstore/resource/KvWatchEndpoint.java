package space.maatini.kvstore.resource;

import io.smallrye.mutiny.Multi;
import space.maatini.kvstore.dto.KvEntryDto;
import space.maatini.kvstore.entity.KvEntry;
import space.maatini.kvstore.service.KvService;
import space.maatini.kvstore.service.KvWatchService;
import jakarta.inject.Inject;
import jakarta.websocket.*;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * WebSocket endpoint for watching KV store changes.
 * 
 * Connect to: ws://host:port/api/v1/kv/watch/{bucket}
 * Or: ws://host:port/api/v1/kv/watch/{bucket}/{key}
 * 
 * Query params:
 * - since: Revision number to start watching from (default: 0 = only future
 * changes)
 * - replay: If true, replay all history first (default: false)
 */
@ServerEndpoint("/api/v1/kv/watch/{bucket}")
public class KvWatchEndpoint {

    private static final Logger LOG = Logger.getLogger(KvWatchEndpoint.class);

    @Inject
    KvWatchService watchService;

    @Inject
    KvService kvService;

    @OnOpen
    public void onOpen(Session session, @PathParam("bucket") String bucket) {
        LOG.infof("Watch session opened: %s for bucket: %s", session.getId(), bucket);

        // Parse query parameters
        String sinceParam = session.getRequestParameterMap()
                .getOrDefault("since", List.of("0")).get(0);
        String replayParam = session.getRequestParameterMap()
                .getOrDefault("replay", List.of("false")).get(0);

        long since = 0;
        try {
            since = Long.parseLong(sinceParam);
        } catch (NumberFormatException e) {
            LOG.warnf("Invalid 'since' parameter: %s", sinceParam);
        }

        boolean replay = Boolean.parseBoolean(replayParam);

        // Subscribe to bucket watches
        watchService.subscribeBucket(session, bucket, since);

        // If replay requested, send history
        if (replay) {
            replayHistory(session, bucket, since);
        }

        // Send connection confirmation
        try {
            session.getBasicRemote().sendText(
                    "{\"type\":\"connected\",\"bucket\":\"" + bucket + "\",\"since\":" + since + "}");
        } catch (Exception e) {
            LOG.warn("Failed to send connection confirmation", e);
        }
    }

    @OnClose
    public void onClose(Session session) {
        LOG.infof("Watch session closed: %s", session.getId());
        watchService.unsubscribe(session);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        LOG.warnf("Watch session error: %s - %s", session.getId(), error.getMessage());
        watchService.unsubscribe(session);
    }

    @OnMessage
    public void onMessage(Session session, String message) {
        // Handle client messages (e.g., ping/pong, unsubscribe requests)
        LOG.debugf("Received message from %s: %s", session.getId(), message);

        if ("ping".equals(message)) {
            try {
                session.getBasicRemote().sendText("pong");
            } catch (Exception e) {
                LOG.warn("Failed to send pong", e);
            }
        }
    }

    private void replayHistory(Session session, String bucket, long since) {
        kvService.listKeys(bucket)
                .onItem().transformToMulti(keys -> Multi.createFrom().iterable(keys))
                .onItem().transformToUniAndConcatenate(key -> kvService.getHistory(bucket, key, 100))
                .subscribe().with(
                        history -> {
                            try {
                                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                                for (KvEntry entry : history) {
                                    if (entry.revision > since) {
                                        KvEntryDto.WatchEvent event = KvEntryDto.WatchEvent.from(entry, bucket);
                                        session.getAsyncRemote().sendText(mapper.writeValueAsString(event));
                                    }
                                }
                            } catch (Exception e) {
                                LOG.error("Failed to replay history segment", e);
                            }
                        },
                        error -> LOG.error("Error fetching history for replay", error));
    }
}
