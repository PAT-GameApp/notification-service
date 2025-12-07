package com.cognizant.notificationService.model;

import lombok.Data;

@Data
public class Booking {
    private String bookingId;
    private long userId;
    private long gameId;
    private long allotmentId;
    private String bookingLocation;
}
