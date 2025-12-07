package com.cognizant.notificationService.exception;

import feign.FeignException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mail.MailException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(FeignException.class)
    public ResponseEntity<Map<String,Object>> handleFeignException(FeignException ex){
        Map<String, Object> response=new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "User service unavailable or invalid response");
        response.put("status", Optional.of(ex.status()));
        response.put("message",ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler(MailException.class)
    public ResponseEntity<Map<String,Object>> handleMailException(MailException ex){
        Map<String, Object> response=new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Failed to send email");
        response.put("message",ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String,Object>> handleGeneralException(Exception ex){
        Map<String, Object> response=new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("error", "Unexpected error");
        response.put("message",ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
