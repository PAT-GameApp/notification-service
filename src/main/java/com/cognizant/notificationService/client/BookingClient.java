package com.cognizant.notificationService.client;

import com.cognizant.notificationService.model.Booking;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@FeignClient(name = "booking-service")
public interface BookingClient {
    // Fetch all bookings from booking-service, aligned with BookingServiceController
    // BookingServiceController defines:
    // @GetMapping("/")
    @GetMapping("/")
    List<Booking> getAllBookings();
}
