package com.cognizant.notificationService.model;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
public class Booking {
    // Mirror key fields from booking-service Booking entity for notifications
    private Long bookingId;
    private Long userId;
    private Long gameId;
    private List<Long> playerIds;
    private Long allotmentId;
    private Long equipmentId;
    private String locationId;
    private LocalDateTime bookingStartTime;
    private LocalDateTime bookingEndTime;
    private LocalDateTime createdAt;
    private LocalDateTime modifiedAt;
}
