package com.cognizant.notificationService.client;

import com.cognizant.notificationService.model.Booking;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@FeignClient(name = "booking-service")
public interface BookingClient {
    @GetMapping("/bookings/all")
    List<Booking> getAllBookings();
}
