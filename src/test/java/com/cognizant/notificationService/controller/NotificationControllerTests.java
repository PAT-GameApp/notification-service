package com.cognizant.notificationService.controller;

import com.cognizant.notificationService.model.Notification;
import com.cognizant.notificationService.service.BookingStoreService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class NotificationControllerTests {

    @Autowired
    MockMvc mvc;

    @Autowired
    BookingStoreService store;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void latestEndpointReturnsBroadcastedNotifications() throws Exception {
        // Arrange
        store.broadcastNotification("hello-from-test");

        // Act
        String json = mvc.perform(get("/notifications/latest")
                .param("limit", "10")
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        List<Notification> notifications = objectMapper.readValue(json, new TypeReference<>() {
        });

        // Assert
        assertThat(notifications).isNotEmpty();
        assertThat(notifications.get(0).getMessage()).contains("hello-from-test");
        assertThat(notifications.get(0).getTimestamp()).isNotNull();
        assertThat(notifications.get(0).getId()).isNotBlank();
    }
}
