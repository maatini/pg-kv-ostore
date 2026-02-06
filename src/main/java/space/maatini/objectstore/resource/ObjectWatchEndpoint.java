package space.maatini.objectstore.resource;

import space.maatini.objectstore.service.ObjectWatchService;
import jakarta.inject.Inject;
import jakarta.websocket.*;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import org.jboss.logging.Logger;

/**
 * WebSocket endpoint for watching Object Store changes.
 * 
 * Connect to: ws://host:port/api/v1/objects/watch/{bucket}
 */
@ServerEndpoint("/api/v1/objects/watch/{bucket}")
public class ObjectWatchEndpoint {

    private static final Logger LOG = Logger.getLogger(ObjectWatchEndpoint.class);

    @Inject
    ObjectWatchService watchService;

    @OnOpen
    public void onOpen(Session session, @PathParam("bucket") String bucket) {
        LOG.infof("Object watch session opened: %s for bucket: %s", session.getId(), bucket);

        // Subscribe to bucket watches
        watchService.subscribeBucket(session, bucket);

        // Send connection confirmation
        try {
            session.getBasicRemote().sendText(
                    "{\"type\":\"connected\",\"bucket\":\"" + bucket + "\"}");
        } catch (Exception e) {
            LOG.warn("Failed to send connection confirmation", e);
        }
    }

    @OnClose
    public void onClose(Session session) {
        LOG.infof("Object watch session closed: %s", session.getId());
        watchService.unsubscribe(session);
    }

    @OnError
    public void onError(Session session, Throwable error) {
        LOG.warnf("Object watch session error: %s - %s", session.getId(), error.getMessage());
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
}
