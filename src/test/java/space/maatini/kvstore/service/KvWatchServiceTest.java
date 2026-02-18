package space.maatini.kvstore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import space.maatini.kvstore.dto.KvEntryDto;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.websocket.RemoteEndpoint;
import jakarta.websocket.Session;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KvWatchService.
 */
@QuarkusTest
class KvWatchServiceTest {

    @Inject
    KvWatchService watchService;

    @Inject
    ObjectMapper objectMapper;

    private Session mockSession;
    private RemoteEndpoint.Async mockAsync;

    @BeforeEach
    void setUp() {
        watchService.clear();
        mockSession = mock(Session.class);
        mockAsync = mock(RemoteEndpoint.Async.class);
        when(mockSession.getId()).thenReturn("test-session-1");
        when(mockSession.isOpen()).thenReturn(true);
        when(mockSession.getAsyncRemote()).thenReturn(mockAsync);
    }

    @AfterEach
    void tearDown() {
        // Ensure session is unsubscribed
        watchService.unsubscribe(mockSession);
    }

    @Test
    void testSubscribeBucket() {
        watchService.subscribeBucket(mockSession, "test-bucket", 0);

        assertEquals(1, watchService.getActiveWatcherCount());
        assertEquals(1, watchService.getWatchedBucketCount());
    }

    @Test
    void testSubscribeKey() {
        watchService.subscribeKey(mockSession, "test-bucket", "test-key", 0);

        assertEquals(1, watchService.getActiveWatcherCount());
        assertEquals(1, watchService.getWatchedKeyCount());
    }

    @Test
    void testUnsubscribe_BucketWatcher() {
        watchService.subscribeBucket(mockSession, "test-bucket", 0);
        assertEquals(1, watchService.getActiveWatcherCount());

        watchService.unsubscribe(mockSession);
        assertEquals(0, watchService.getActiveWatcherCount());
        assertEquals(0, watchService.getWatchedBucketCount());
    }

    @Test
    void testUnsubscribe_KeyWatcher() {
        watchService.subscribeKey(mockSession, "test-bucket", "test-key", 0);
        assertEquals(1, watchService.getActiveWatcherCount());

        watchService.unsubscribe(mockSession);
        assertEquals(0, watchService.getActiveWatcherCount());
        assertEquals(0, watchService.getWatchedKeyCount());
    }

    @Test
    void testNotifyChange_BucketWatcher() {
        watchService.subscribeBucket(mockSession, "test-bucket", 0);

        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "any-key");
        watchService.notifyChange(event);

        // Verify the event was sent
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAsync).sendText(messageCaptor.capture(), any());

        String sentMessage = messageCaptor.getValue();
        assertNotNull(sentMessage);
        assertTrue(sentMessage.contains("test-bucket"));
        assertTrue(sentMessage.contains("any-key"));
    }

    @Test
    void testNotifyChange_KeyWatcher() {
        watchService.subscribeKey(mockSession, "test-bucket", "specific-key", 0);

        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "specific-key");
        watchService.notifyChange(event);

        // Verify the event was sent
        verify(mockAsync).sendText(anyString(), any());
    }

    @Test
    void testNotifyChange_KeyWatcher_DifferentKey() {
        watchService.subscribeKey(mockSession, "test-bucket", "specific-key", 0);

        // Send event for a different key
        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "other-key");
        watchService.notifyChange(event);

        // Verify no event was sent (watching different key)
        verify(mockAsync, never()).sendText(anyString(), any());
    }

    @Test
    void testNotifyChange_BucketWatcher_DifferentBucket() {
        watchService.subscribeBucket(mockSession, "bucket-1", 0);

        // Send event for a different bucket
        KvEntryDto.WatchEvent event = createTestEvent("bucket-2", "key");
        watchService.notifyChange(event);

        // Verify no event was sent (watching different bucket)
        verify(mockAsync, never()).sendText(anyString(), any());
    }

    @Test
    void testNotifyChange_SkipsOldRevisions() {
        // Subscribe with since=5
        watchService.subscribeBucket(mockSession, "test-bucket", 5);

        // Send event with revision 3 (should be skipped)
        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "key");
        event.revision = 3L;
        watchService.notifyChange(event);

        // Verify no event was sent
        verify(mockAsync, never()).sendText(anyString(), any());

        // Send event with revision 6 (should be sent)
        event.revision = 6L;
        watchService.notifyChange(event);

        // Verify event was sent
        verify(mockAsync).sendText(anyString(), any());
    }

    @Test
    void testNotifyChange_ClosedSession() {
        when(mockSession.isOpen()).thenReturn(false);
        watchService.subscribeBucket(mockSession, "test-bucket", 0);

        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "key");
        watchService.notifyChange(event);

        // Session should be unsubscribed
        assertEquals(0, watchService.getActiveWatcherCount());
    }

    @Test
    void testMultipleWatchers() {
        Session session2 = mock(Session.class);
        RemoteEndpoint.Async async2 = mock(RemoteEndpoint.Async.class);
        when(session2.getId()).thenReturn("test-session-2");
        when(session2.isOpen()).thenReturn(true);
        when(session2.getAsyncRemote()).thenReturn(async2);

        watchService.subscribeBucket(mockSession, "test-bucket", 0);
        watchService.subscribeBucket(session2, "test-bucket", 0);

        assertEquals(2, watchService.getActiveWatcherCount());

        KvEntryDto.WatchEvent event = createTestEvent("test-bucket", "key");
        watchService.notifyChange(event);

        // Both sessions should receive the event
        verify(mockAsync).sendText(anyString(), any());
        verify(async2).sendText(anyString(), any());

        // Cleanup
        watchService.unsubscribe(session2);
    }

    @Test
    void testResubscribe() {
        watchService.subscribeBucket(mockSession, "bucket-1", 0);
        assertEquals(1, watchService.getWatchedBucketCount());

        // Subscribing again should update the subscription
        watchService.subscribeBucket(mockSession, "bucket-2", 0);

        assertEquals(1, watchService.getActiveWatcherCount());
    }

    private KvEntryDto.WatchEvent createTestEvent(String bucket, String key) {
        KvEntryDto.WatchEvent event = new KvEntryDto.WatchEvent();
        event.type = "PUT";
        event.bucket = bucket;
        event.key = key;
        event.value = "dGVzdA=="; // base64 for "test"
        event.revision = 1L;
        event.timestamp = OffsetDateTime.now();
        return event;
    }
}
