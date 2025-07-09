package com.kafka.controller;

import com.kafka.dto.Customer;
import com.kafka.service.KafkaMessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Iterator;
import java.util.stream.IntStream;

@RequestMapping("/events")
@RestController
public class EventController {

    private final KafkaMessageProducer kafkaMessageProducer;

    public EventController(KafkaMessageProducer kafkaMessageProducer) {
        this.kafkaMessageProducer = kafkaMessageProducer;
    }

    @GetMapping("/send/{message}")
    public ResponseEntity<?> sendMessage(@PathVariable String message) {
        try {
            IntStream.iterate(0, i -> i + 1)
                    .limit(100000)
                    .forEach(i -> {
                        kafkaMessageProducer.sendMessage(message + " - " + (i + 1));
                    });
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
    }


    //Send for all partition (Uses round robin algorithm)
    @PostMapping("/send/customer")
    public ResponseEntity<?> sendCustomer(@RequestBody Customer customer) {
        try {

            IntStream.range(0, 100)
                    .forEach(i -> {
                        customer.setId(i);
                        kafkaMessageProducer.sendCustomer(customer, 0);
                    });

            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
    }

    //Send for particular partition
    @PostMapping("/send/customer/{no}")
    public ResponseEntity<?> sendCustomerSecondPartition(@RequestBody Customer customer, @PathVariable int no) {
        try {

            IntStream.range(0, 100)
                    .forEach(i -> {
                        customer.setId(i);
                        kafkaMessageProducer.sendCustomer(customer, no);
                    });
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
    }

    // Error handling for consumer if topic failed to process in consumer(DLT :  Dead Letter Topics)
    @PostMapping("/send/customer/dlt/{no}")
    public ResponseEntity<?> sendCustomer(@RequestBody Customer customer, @PathVariable int no) {
        try {
            kafkaMessageProducer.sendCustomer(customer, no);
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message: " + e.getMessage());
        }
    }

}
