package com.cognizant.notificationService.kafka;

import com.cognizant.notificationService.model.Booking;

import com.cognizant.notificationService.service.BookingStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Component
public class BookingProducer {
    private final Logger log= LoggerFactory.getLogger(BookingProducer.class);
    private final KafkaTemplate<String, Booking> kafkaTemplate;
    private final BookingStoreService store;
    private final String topic;

    public BookingProducer(KafkaTemplate<String, Booking> kafkaTemplate, Environment env,BookingStoreService store) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic=env.getProperty("kafka.topic.bookings", "bookings-topic");
        this.store=store;
    }
    public void sendIfChanged(Booking booking){
        if(booking==null || booking.getBookingId()==null) return;
        if(store.isDifferent(booking)){
            log.debug("Skipping publish for booking {} - no change",booking.getBookingId());
            return;
        }

        String key=booking.getBookingId();
        log.info("Publishing booking id= "+ key+" to topic= "+topic);
        kafkaTemplate.send(topic,key,booking).whenComplete((result,ex)->{
            if(ex != null){
                log.error("Failed to send Booking {}", key, ex);
            }else{
                store.markPublished(booking);
                log.debug("Sent booking {} to partition {}",key, result.getRecordMetadata().partition());
            }
        });

    }
}
