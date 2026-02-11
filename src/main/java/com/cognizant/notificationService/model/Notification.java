package com.cognizant.notificationService.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Notification {
    private String id;
    private String message;
    private LocalDateTime timestamp;
    private boolean read;
    private Long userId;
}
