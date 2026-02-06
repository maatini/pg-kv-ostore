package space.maatini.kvstore.resource;

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
 * WebSocket endpoint for watching a specific key in the KV store.
 */
@ServerEndpoint("/api/v1/kv/watch/{bucket}/{key}")
public class KvKeyWatchEndpoint {

    private static final Logger LOG = Logger.getLogger(KvKeyWatchEndpoint.class);

    @Inject
    KvWatchService watchService;

    @Inject
    KvService kvService;

    @OnOpen
    public void onOpen(Session session,
            @PathParam("bucket") String bucket,
            @PathParam("key") String key) {
        LOG.infof("Key watch session opened: %s for %s/%s", session.getId(), bucket, key);

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

        // Subscribe to key watches
        watchService.subscribeKey(session, bucket, key, since);

        // If replay requested, send history
        if (replay) {
            replayKeyHistory(session, bucket, key, since);
        }

        // Send connection confirmation
        try {
            session.getBasicRemote().sendText(
                    "{\"type\":\"connected\",\"bucket\":\"" + bucket +
                            "\",\"key\":\"" + key + "\",\"since\":" + since + "}");
        } catch (Exception e) {
            LOG.warn("Failed to send connection confirmation", e);
        }
    }

    @OnClose
    public void onClose(Session session) {
        LOG.infof("Key watch session closed: %s", session.getId());
        watchService.unsubscribe(session);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        LOG.warnf("Key watch session error: %s - %s", session.getId(), error.getMessage());
        watchService.unsubscribe(session);
    }

    @OnMessage
    public void onMessage(Session session, String message) {
        LOG.debugf("Received message from %s: %s", session.getId(), message);

        if ("ping".equals(message)) {
            try {
                session.getBasicRemote().sendText("pong");
            } catch (Exception e) {
                LOG.warn("Failed to send pong", e);
            }
        }
    }

    private void replayKeyHistory(Session session, String bucket, String key, long since) {
        try {
            List<KvEntry> history = kvService.getHistory(bucket, key, 100);
            for (KvEntry entry : history) {
                if (entry.revision > since) {
                    KvEntryDto.WatchEvent event = KvEntryDto.WatchEvent.from(entry, bucket);
                    session.getBasicRemote().sendText(
                            new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(event));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to replay key history", e);
        }
    }
}
