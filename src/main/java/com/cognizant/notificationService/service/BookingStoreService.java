package com.cognizant.notificationService.service;

import com.cognizant.notificationService.model.Booking;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class BookingStoreService {
    private final ConcurrentMap<String, Booking> bookingMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> bookingHashMap = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void upsertFromConsumer(Booking b) {
        if (b == null || b.getBookingId() == null) return;
        String key = String.valueOf(b.getBookingId());
        bookingMap.put(key, b);
        String hash = computeHashSafe(b);
        if (hash != null) bookingHashMap.put(key, hash);
    }

    public Collection<Booking> getAllBookings() {
        return bookingMap.values();
    }

    public String getLastHash(String bookingId) {
        return bookingHashMap.get(bookingId);
    }

    public void markPublished(Booking b) {
        if (b == null || b.getBookingId() == null) return;
        String key = String.valueOf(b.getBookingId());
        String hash = computeHashSafe(b);
        if (hash != null) bookingHashMap.put(key, hash);
        bookingMap.put(key, b);
    }

    public boolean isDifferent(Booking b) {
        if (b == null || b.getBookingId() == null) return true;
        String newHash = computeHashSafe(b);
        if (newHash == null) return true;
        String key = String.valueOf(b.getBookingId());
        String last = bookingHashMap.get(key);
        return last == null || !last.equals(newHash);
    }

    private String computeHashSafe(Booking b) {
        try {
            String json = objectMapper.writeValueAsString(b);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(json.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            return null;
        }
    }
}
