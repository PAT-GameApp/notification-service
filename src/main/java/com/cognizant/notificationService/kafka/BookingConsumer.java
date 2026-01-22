package com.cognizant.notificationService.kafka;

import com.cognizant.notificationService.service.BookingStoreService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BookingConsumer {
    private static final Logger log = LoggerFactory.getLogger(BookingConsumer.class);
    private final BookingStoreService store;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BookingConsumer(BookingStoreService store) {
        this.store = store;
    }

    /**
     * Consume raw JSON from booking-events topic (BookingEvent payload) and
     * turn CREATED events into user-friendly notifications for the bell icon.
     */
    @KafkaListener(topics = "${kafka.topic.bookings}", groupId = "notification-service-group")
    public void listen(String payload) {
        log.info("Received booking event JSON from Kafka: {}", payload);

        try {
            JsonNode root = objectMapper.readTree(payload);

            String eventType = root.path("eventType").asText(null);
            Long bookingId = root.path("bookingId").isMissingNode() || root.path("bookingId").isNull()
                    ? null
                    : root.path("bookingId").asLong();
            Long userId = root.path("userId").isMissingNode() || root.path("userId").isNull()
                    ? null
                    : root.path("userId").asLong();
            Long gameId = root.path("gameId").isMissingNode() || root.path("gameId").isNull()
                    ? null
                    : root.path("gameId").asLong();

            if (bookingId == null) {
                log.warn("Received booking event without bookingId: {}", payload);
                return;
            }

            // Only push CREATED events to the notification bell
            if (!"CREATED".equalsIgnoreCase(eventType)) {
                log.debug("Ignoring booking event of type {} for bookingId={} ", eventType, bookingId);
                return;
            }

            String notificationMessage = "Booking created successfully";
            // store.broadcastNotification(notificationMessage);

            // This writes to history and broadcasts SSE BOOKING_NOTIFICATION
            store.broadcastNotification(notificationMessage);

        } catch (Exception e) {
            log.error("Failed to process booking event payload: {}", payload, e);
        }
    }
}
