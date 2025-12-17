package com.cognizant.notificationService.client;

import com.cognizant.notificationService.model.User;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "user-service")
public interface UserClient {
    // Align with UserController:
    // @GetMapping("/{id}")
    // public ResponseEntity<User> getUserById(@PathVariable Long id)
    @GetMapping("/{id}")
    User getUserById(@PathVariable("id") Long id);
}
