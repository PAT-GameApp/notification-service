package com.cognizant.notificationService.kafka;

import com.cognizant.notificationService.client.UserClient;
import com.cognizant.notificationService.model.User;
import com.cognizant.notificationService.model.Booking;
import com.cognizant.notificationService.service.BookingStoreService;
import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class BookingConsumer {
    private static final Logger log = LoggerFactory.getLogger(BookingConsumer.class);
    private final BookingStoreService store;

    public BookingConsumer(BookingStoreService store) {
        this.store = store;
    }

    @KafkaListener(topics = "${kafka.topic.bookings}", groupId = "notification-service-group")
    public void listen(Booking booking) {
        log.info("Received booking message from Kafka: {}", booking);
        if (booking == null || booking.getBookingId() == null) {
            log.warn("Received null or invalid booking message: {}", booking);
            return;
        }
        log.debug("Consumed booking id {}", booking.getBookingId());
        store.upsertFromConsumer(booking);
        // At this point you can plug in email/SMS or other notification logic if needed.
    }
}
