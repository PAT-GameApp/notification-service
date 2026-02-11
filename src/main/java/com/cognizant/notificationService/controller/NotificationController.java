package com.cognizant.notificationService.controller;

import com.cognizant.notificationService.model.Booking;
import com.cognizant.notificationService.model.Notification;
import com.cognizant.notificationService.service.BookingStoreService;
import com.cognizant.notificationService.service.BookingSyncService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collection;
import java.util.List;

@RequestMapping("/notifications")
@RestController
public class NotificationController {
    private final BookingSyncService syncService;
    private final BookingStoreService store;

    public NotificationController(BookingStoreService store, BookingSyncService syncService) {
        this.store = store;
        this.syncService = syncService;
    }

    @PostMapping("/sync")
    public String sync() {
        syncService.syncAllBookings();
        return "Sync Triggered";
    }

    @GetMapping("/bookings")
    public Collection<Booking> getAllBookingsForUI() {
        return store.getAllBookings();
    }

    /**
     * Fetch a small notification history to populate the bell dropdown.
     * Most recent notifications come first.
     */
    @GetMapping("/latest")
    public List<Notification> getLatest(
            @RequestParam(name = "limit", defaultValue = "20") int limit,
            @RequestParam(name = "userId", required = false) Long userId) {
        return store.getLatestNotifications(limit, userId);
    }

    @DeleteMapping("/clear")
    public String clear(@RequestParam(name = "userId", required = false) Long userId) {
        store.clearNotifications(userId);
        return "Notifications cleared for user " + userId;
    }

    // ===== NEW: SSE stream for bell icon / real-time notifications =====

    /**
     * Frontend will open an EventSource to this URL to receive real-time
     * notifications.
     * Example: new
     * EventSource('http://localhost:8082/notifications/stream?userId=123')
     */
    // @CrossOrigin(origins = "http://localhost:3000") // if needed
    @GetMapping("/stream")
    public SseEmitter subscribe(@RequestParam(name = "userId", required = false) Long userId) {
        return store.addSubscriber(userId);
    }

    /**
     * Frontend-friendly alias (matches notification-app style paths).
     *
     * This lets the UI use a stable URL while we integrate it into the Vite app.
     */
    @GetMapping("/api/notifications/stream")
    public SseEmitter subscribeAlias(@RequestParam(name = "userId", required = false) Long userId) {
        return store.addSubscriber(userId);
    }

    // Optional: a simple test endpoint to push a manual notification
    @PostMapping("/test-notification")
    public String testNotification(@RequestParam(defaultValue = "Test notification") String message,
            @RequestParam(required = false) Long userId) {
        store.broadcastNotification(message, userId);
        return "Notification broadcasted";
    }
}
