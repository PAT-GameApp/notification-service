package com.cognizant.notificationService.service;

import com.cognizant.notificationService.model.Booking;
import com.cognizant.notificationService.model.Notification;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class BookingStoreService {
    private static final Logger log = LoggerFactory.getLogger(BookingStoreService.class);

    private final ConcurrentMap<String, Booking> bookingMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> bookingHashMap = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // --- NEW: SSE subscribers registry ---
    private final Set<SseEmitter> subscribers = new CopyOnWriteArraySet<>();

    // --- NEW: notification history buffer (last N) ---
    private static final int NOTIFICATION_HISTORY_LIMIT = 50;
    private final Deque<Notification> notificationHistory = new ConcurrentLinkedDeque<>();

    public void upsertFromConsumer(Booking b) {
        if (b == null || b.getBookingId() == null)
            return;
        String key = String.valueOf(b.getBookingId());
        bookingMap.put(key, b);
        String hash = computeHashSafe(b);
        if (hash != null)
            bookingHashMap.put(key, hash);
    }

    public Collection<Booking> getAllBookings() {
        return bookingMap.values();
    }

    public List<Notification> getLatestNotifications(int limit) {
        int effectiveLimit = Math.max(0, Math.min(limit, NOTIFICATION_HISTORY_LIMIT));
        return notificationHistory.stream().limit(effectiveLimit).toList();
    }

    public String getLastHash(String bookingId) {
        return bookingHashMap.get(bookingId);
    }

    public void markPublished(Booking b) {
        if (b == null || b.getBookingId() == null)
            return;
        String key = String.valueOf(b.getBookingId());
        String hash = computeHashSafe(b);
        if (hash != null)
            bookingHashMap.put(key, hash);
        bookingMap.put(key, b);
    }

    public boolean isDifferent(Booking b) {
        if (b == null || b.getBookingId() == null)
            return true;
        String newHash = computeHashSafe(b);
        if (newHash == null)
            return true;
        String key = String.valueOf(b.getBookingId());
        String last = bookingHashMap.get(key);
        return last == null || !last.equals(newHash);
    }

    private String computeHashSafe(Booking b) {
        try {
            String json = objectMapper.writeValueAsString(b);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(json.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            return null;
        }
    }

    // ===== SSE support for notifications =====

    /**
     * Register a new SSE subscriber (frontend bell icon / notifications UI).
     */
    public SseEmitter addSubscriber(Long userId) {
        SseEmitter emitter = new SseEmitter(0L); // no timeout, or configure as needed
        subscribers.add(emitter);
        log.info("New SSE subscriber added (userId={}), total subscribers={}", userId, subscribers.size());

        emitter.onCompletion(() -> subscribers.remove(emitter));
        emitter.onTimeout(() -> subscribers.remove(emitter));
        emitter.onError(e -> subscribers.remove(emitter));

        // Optionally send an initial event so client knows connection is alive
        try {
            emitter.send(SseEmitter.event()
                    .name("INIT")
                    .data("connected"));
        } catch (IOException e) {
            log.warn("Failed to send INIT event to subscriber", e);
        }

        return emitter;
    }

    /**
     * Broadcast a notification message to all connected subscribers.
     */
    public void broadcastNotification(Object notificationPayload) {
        if (subscribers.isEmpty()) {
            log.debug("No SSE subscribers to broadcast notification");
            return;
        }
        log.info("Broadcasting notification '{}' to {} subscribers",
                notificationPayload, subscribers.size());

        for (SseEmitter emitter : subscribers) {
            try {
                emitter.send(SseEmitter.event()
                        .name("BOOKING_NOTIFICATION")
                        .data(notificationPayload));
            } catch (IOException e) {
                log.warn("Error sending notification to one subscriber, removing emitter", e);
                emitter.completeWithError(e);
                subscribers.remove(emitter);
            }
        }
    }

    /**
     * Convenience API mirroring `notification-app`: build a standard Notification
     * object and broadcast it.
     */
    public void broadcastNotification(String message) {
        Notification notification = new Notification(
                UUID.randomUUID().toString(),
                message,
                LocalDateTime.now(),
                false);
        // push into history (most recent first)
        notificationHistory.addFirst(notification);
        while (notificationHistory.size() > NOTIFICATION_HISTORY_LIMIT) {
            notificationHistory.removeLast();
        }
        broadcastNotification((Object) notification);
    }
}
