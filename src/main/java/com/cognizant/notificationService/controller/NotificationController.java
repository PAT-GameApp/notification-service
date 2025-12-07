package com.cognizant.notificationService.controller;

import com.cognizant.notificationService.model.Booking;
import com.cognizant.notificationService.service.BookingStoreService;
import com.cognizant.notificationService.service.BookingSyncService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RequestMapping("/notifications")
@RestController
public class NotificationController {
    private final BookingSyncService syncService;
    private final BookingStoreService store;
    public NotificationController(BookingStoreService store, BookingSyncService syncService){
        this.store=store;
        this.syncService=syncService;
    }
    @PostMapping("/sync")
    public String sync(){
        syncService.syncAllBookings();
        return "Sync Triggered";
    }

    @GetMapping("/bookings")
    public Collection<Booking> getAllBookingsForUI(){
        return store.getAllBookings();
    }
}
