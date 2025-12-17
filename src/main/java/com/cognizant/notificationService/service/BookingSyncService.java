package com.cognizant.notificationService.service;

import com.cognizant.notificationService.client.BookingClient;
import com.cognizant.notificationService.kafka.BookingProducer;
import com.cognizant.notificationService.model.Booking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BookingSyncService {
    private static final Logger log = LoggerFactory.getLogger(BookingSyncService.class);
    private final BookingClient bookingClient;
    private final BookingProducer producer;

    public BookingSyncService(BookingClient bookingClient, BookingProducer producer) {
        this.bookingClient = bookingClient;
        this.producer = producer;
    }

    public void syncAllBookings() {
        log.info("Fetching bookings from booking-service via Feign client");
        List<Booking> bookings = bookingClient.getAllBookings();
        if (bookings == null || bookings.isEmpty()) {
            log.info("No bookings returned from booking-service");
            return;
        }
        bookings.forEach(producer::sendIfChanged);
        log.info("Sync complete (processed {} bookings)", bookings.size());
    }
}
